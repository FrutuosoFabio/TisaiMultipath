using TisaiMultipath.Helpers;
using TisaiMultipath.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TisaiMultipath.Services
{
    public static class ServerService
    {
        // ConcurrentDictionary (NÃO Dictionary): routes é tocado por 3 threads —
        // FwService (TryAdd), StartServer (TryRemove ao expirar) e BckService (foreach).
        // Com Dictionary sem lock, o churn de reconexão (disconnect/reconnect) causava
        // "Collection was modified" no foreach do BckService (dropava a resposta do WG)
        // ou corrompia o dict → retorno travava PERMANENTE até restart. ConcurrentDictionary
        // tem iteração snapshot (não lança) e Add/Remove atômicos.
        private static ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>? routes = new ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();

        // Índice O(1) (IP,Port)->Route canônico. ConcurrentDictionary.ContainsKey é O(1)
        // mas NÃO retorna o objeto-chave armazenado; sem isso o FwService fazia
        // routes.Keys.FirstOrDefault(r => r.Equals(...)) = SCAN LINEAR O(N) por pacote,
        // VÁRIAS vezes (registro + 3B + 1B + seq). Com muitas rotas a thread única do
        // FwService afogava no scan e o buffer UDP estourava (drop ~58% em 100 rotas a
        // 6400pps, com CPU em só 23%). Com o índice: 1 lookup O(1) por pacote.
        private static ConcurrentDictionary<Route, Route> _routeIndex = new ConcurrentDictionary<Route, Route>();

        // Demux do DOWNSTREAM por cliente: índice WG do cliente -> as rotas (paths) DELE.
        // Sem isso, o BckService faz broadcast de CADA resposta do WG pra TODAS as rotas
        // (O(N) waste: com M players x 2 paths = 2M envios por pacote, sendo 2 úteis). Aqui:
        // aprende o índice no handshake INIT (sender_index do cliente) e no downstream casa
        // pelo receiver_index -> manda só pras 2 rotas do dono. Índice desconhecido (proxy
        // subiu no meio da sessão) -> fallback broadcast (seguro, não quebra nada).
        private static readonly ConcurrentDictionary<uint, ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>> _clientIndexRoutes
            = new ConcurrentDictionary<uint, ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>>();

        private static UdpClient fwClient = Utils.CreateDualStackUdpClient(0);
        private static UdpClient bckClient = Utils.CreateDualStackUdpClient(0);

        // Destino do forward (WG/next hop), parseado 1x — FwService envia inline pra cá.
        private static IPEndPoint? _destEndpoint;
        // SocketAddress cacheado do DEST (v6-mapped p/ socket dual-stack) — usado no SendTo
        // zero-alloc do forward (evita serializar o IPEndPoint a cada pacote).
        private static SocketAddress? _destSockAddr;

        // -------- Seq dedup protocol (header de 6 bytes prepended pelos clientes) --------
        // Formato: [0xAA][seq3][seq2][seq1][seq0][0x55] + payload WG. Ver MULTIPATH-SEQ-DEDUP.md.
        // Server na ponta SP dedupe por seq + strip header antes de forwardar pro WG kernel.
        // Na resposta, server adiciona NOVO header (com _serverSeq incremental).
        // MAO PoP hops NAO precisam — desativa via SeqEnabled=false (forward as-is).
        private const byte SEQ_MAGIC_HEAD = 0xAA;
        private const byte SEQ_MAGIC_TAIL = 0x55;
        private const int SEQ_HEADER_SIZE = 6;
        private const int RECV_SEQ_WINDOW = 4096;
        private static bool _seqEnabled = false;
        private static uint _serverSeq = 0;

        // Janela de dedup do uplink, 2-bucket rotation (sem Clear() total, que abria
        // janela de race -> OOO; mesmo fix do cliente MultipathRouterService).
        private sealed class SeqWindow
        {
            public HashSet<uint> Current = new HashSet<uint>();
            public HashSet<uint> Previous = new HashSet<uint>();
            public DateTime LastSeen = DateTime.UtcNow;
        }
        // Dedup POR SESSAO WG (receiver_index), NAO global. Cada cliente tem seq
        // proprio comecando em 0; uma janela global faria o seq=N do cliente B ser
        // visto como duplicata do seq=N do cliente A -> drop de pacote legitimo.
        // As copias multipath do MESMO cliente compartilham o receiver_index (e o
        // seq), entao casam na mesma janela; clientes distintos tem indices distintos.
        private static readonly Dictionary<uint, SeqWindow> _seqWindows = new Dictionary<uint, SeqWindow>();
        private static readonly object _recvSeqLock = new object();
        private static long _totalSeqDeduped = 0;
        private static long _totalSeqPassed = 0;
        private static long _totalLegacy = 0;

        public static void StartServer(string port, string destination, bool seqEnabled = false)
        {
            _seqEnabled = seqEnabled;
            Console.WriteLine($"Starting server on port {port}... seqDedup={(seqEnabled ? "ON" : "OFF")}");

            // LongRunning (NÃO Task.Run nem new Thread): FwService/BckService bloqueiam
            // pra sempre em Receive(). LongRunning dá thread DEDICADA fora do ThreadPool
            // (sem prender workers -> sem starvation). Continua Task -> exceção não-tratada
            // falha a task silenciosa, NÃO derruba o processo (new Thread crashava via
            // EINVAL no LatencyThread em hosts com loopback dual-stack diferente, ex akm).
            // DEST parseado uma vez (forward inline, sem re-resolver por pacote).
            _destEndpoint = IPEndPoint.Parse(destination);
            // v6-mapped pro SendTo zero-alloc no socket dual-stack (v4 puro daria EINVAL).
            var destMapped = _destEndpoint.Address.AddressFamily == AddressFamily.InterNetwork
                ? new IPEndPoint(_destEndpoint.Address.MapToIPv6(), _destEndpoint.Port)
                : _destEndpoint;
            _destSockAddr = destMapped.Serialize();

            // N threads de RECEIVE com SO_REUSEPORT (kernel balanceia por fluxo -> escala com
            // cores). Cada cliente (4-tupla fixa) cai sempre na MESMA thread -> estado por-cliente
            // (Route) é single-thread; só os dicts compartilhados precisam ser thread-safe (já são).
            // socks[0] = socket de RETORNO (BckService/LatencyThread enviam respostas por ele).
            // N = nº de cores (1 vCPU -> 1 thread, sem regressão); override via TISAI_FW_THREADS.
            int fwThreads = Math.Max(1, Environment.ProcessorCount);
            if (int.TryParse(Environment.GetEnvironmentVariable("TISAI_FW_THREADS"), out int envT) && envT > 0)
                fwThreads = envT;
            Console.WriteLine($"FwService: {fwThreads} thread(s) SO_REUSEPORT na porta {port}");
            for (int i = 0; i < fwThreads; i++)
            {
                var sock = Utils.CreateReusePortUdpClient(int.Parse(port));
                if (i == 0) fwClient = sock;
                Task.Factory.StartNew(() => FwService(sock), TaskCreationOptions.LongRunning);
            }

            Task.Factory.StartNew(() => BckService(), TaskCreationOptions.LongRunning);

            if (routes == null)
                routes = new ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();

            Utils.PrintRouteStates(routes.Keys.ToList() ?? new List<Route>(), false);
            while (true)
            {
                Utils.PrintRouteStates(routes.Keys.ToList(), true);
                Thread.Sleep(1500);

                // 15s ao inves de 5s — sob trafego alto o ping (1B) pode atrasar atras de
                // rajadas WG e o Route do client "morria" no server, dropando a sessao.
                foreach(Route r in routes.Keys.ToList())
                {
                    if((DateTime.UtcNow - r.LastPing).TotalSeconds > 15)
                    {
                        routes.TryRemove(r, out _);
                        _routeIndex.TryRemove(r, out _);  // mantém o índice O(1) em sync com routes
                        // Tira a rota do demux de downstream também (senão vira rota fantasma).
                        if (r.WgClientIndex != 0 && _clientIndexRoutes.TryGetValue(r.WgClientIndex, out var crs))
                        {
                            crs.TryRemove(r, out _);
                            if (crs.IsEmpty) _clientIndexRoutes.TryRemove(r.WgClientIndex, out _);
                        }
                    }
                }

                // Poda janelas de dedup de sessoes WG inativas (>60s) — evita vazar
                // memoria com receiver_index velhos (rekey cria indice novo).
                lock (_recvSeqLock)
                {
                    foreach (var idx in _seqWindows
                                 .Where(kv => (DateTime.UtcNow - kv.Value.LastSeen).TotalSeconds > 60)
                                 .Select(kv => kv.Key).ToList())
                    {
                        _seqWindows.Remove(idx);
                    }
                }
            }
        }

        //Received from MP Client to Server. UMA instância por thread SO_REUSEPORT (recvSocket).
        private static void FwService(UdpClient recvSocket)
        {
            Console.WriteLine("[Server] FwService thread is running...");

            if (routes == null)
                routes = new ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();

            // BATCH (recvmmsg): TISAI_BATCH=N (>1) pega N datagramas por syscall — corta a
            // viagem user<->kernel que domina em pps alto. Mesma lógica por pacote (ProcessPacket).
            int batch = 0;
            int.TryParse(Environment.GetEnvironmentVariable("TISAI_BATCH"), out batch);
            if (batch > 1)
            {
                Console.WriteLine($"[Server] FwService modo BATCH recvmmsg N={batch}");
                var br = new BatchReceiver(recvSocket.Client, batch);
                while (true)
                {
                    try { br.Receive((d, l, ip, port) => ProcessPacket(d, l, ip, port, recvSocket)); }
                    catch (Exception ex) { Console.WriteLine(ex.ToString()); }
                }
            }

            // Buffer + SocketAddress REUSADOS (zero-alloc no receive: sem new byte[] por pacote).
            byte[] rxbuf = new byte[2048];
            byte[] ipbuf = new byte[16];
            var rxAddr = new SocketAddress(AddressFamily.InterNetworkV6);
            while (true)
            {
                try
                {
                    int len = recvSocket.Client.ReceiveFrom(rxbuf.AsSpan(), SocketFlags.None, rxAddr);
                    // sockaddr_in6 no SocketAddress: porta BE @2, addr @8 (v6-mapped p/ v4).
                    int port = (rxAddr[2] << 8) | rxAddr[3];
                    for (int k = 0; k < 16; k++) ipbuf[k] = rxAddr[8 + k];
                    ProcessPacket(rxbuf, len, new IPAddress(ipbuf), port, recvSocket);
                }
                catch (Exception ex) { Console.WriteLine(ex.ToString()); }
            }
        }

        // Processa UM datagrama do cliente: registra rota, aprende índice WG, trata controle
        // (3/2/1B), dedup seq, forwarda inline pro WG. Chamado pelo caminho single E pelo batch.
        // rawSrcAddr = IP cru (v6-mapped p/ clientes v4); normaliza p/ a chave, usa cru no bounce.
        private static void ProcessPacket(byte[] data, int len, IPAddress rawSrcAddr, int srcPort, UdpClient recvSocket)
        {
            // 'data' pode ser um buffer REUSADO maior que o pacote -> usar 'len', NÃO data.Length.
            IPAddress clientAddr = Utils.NormalizeAddress(rawSrcAddr);

            // Lookup O(1) do Route canônico via índice.
            Route _routeKey = new Route() { IPAddress = clientAddr, Port = srcPort };
            if (!_routeIndex.TryGetValue(_routeKey, out Route? clientRoute) || clientRoute == null)
            {
                BlockingCollection<(byte[], UdpClient?)> _queue = new BlockingCollection<(byte[], UdpClient?)>();
                clientRoute = new Route(_queue, null, fwClient) { IPAddress = clientAddr, Port = srcPort };
                routes!.TryAdd(clientRoute, _queue);
                _routeIndex.TryAdd(clientRoute, clientRoute);
            }
            clientRoute.LastPing = DateTime.UtcNow;

                    // Aprende o índice WG do cliente no handshake INIT (type 1): o sender_index
                    // (offset 4 do pacote WG) é o mesmo que o WG carimba como receiver_index em
                    // todo downstream desse cliente -> permite o demux no BckService (manda só
                    // pras rotas DELE). wgOff pula o header seq se presente. Guard de tamanho
                    // garante que controle (3/2/1 byte) não entra aqui.
                    {
                        bool isSeqPkt = _seqEnabled && len >= SEQ_HEADER_SIZE
                                        && data[0] == SEQ_MAGIC_HEAD && data[5] == SEQ_MAGIC_TAIL;
                        int wgOff0 = isSeqPkt ? SEQ_HEADER_SIZE : 0;
                        if (len >= wgOff0 + 8 && data[wgOff0] == 1)
                        {
                            uint cidx = (uint)data[wgOff0 + 4]
                                      | ((uint)data[wgOff0 + 5] << 8)
                                      | ((uint)data[wgOff0 + 6] << 16)
                                      | ((uint)data[wgOff0 + 7] << 24);
                            if (cidx != 0 && routes != null && routes.TryGetValue(clientRoute, out var cq))
                            {
                                clientRoute.WgClientIndex = cidx;
                                var set = _clientIndexRoutes.GetOrAdd(cidx,
                                    _ => new ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>());
                                set.TryAdd(clientRoute, cq);
                            }
                        }
                    }

                    // control de 3 bytes (enable/disable da rota)
                    if (len == 3)
                    {
                        clientRoute.SetRouteActive(data[2]);
                        return;
                    }

                    // ping de 2 bytes volta (bounce) — envia pro IP cru (v6-mapped) no socket dual-stack
                    if (len == 2)
                    {
                        recvSocket.Send(new byte[] { data[0] }, 1, new IPEndPoint(rawSrcAddr, srcPort));
                        return;
                    }

                    if(len == 1)
                    {
                        clientRoute.CalculateLatency(data[0]);
                    }

                    // -------- SEQ DEDUP (se ativado e header magic detectado) --------
                    int sendOff = 0; // strip do header seq vira só um offset (zero-alloc, sem new byte[])
                    if (_seqEnabled
                        && len >= SEQ_HEADER_SIZE
                        && data[0] == SEQ_MAGIC_HEAD
                        && data[5] == SEQ_MAGIC_TAIL)
                    {
                        // Tipo da msg WG vem logo APÓS o header seq de 6 bytes:
                        // 1=handshake init, 2=handshake response, 3=cookie reply, 4=transport data.
                        byte wgType = len > SEQ_HEADER_SIZE ? data[SEQ_HEADER_SIZE] : (byte)0;
                        bool isHandshake = wgType >= 1 && wgType <= 3;

                        // Marca este route como falando seq → respostas vão com header (O(1), já temos clientRoute)
                        clientRoute.UsesSeq = true;

                        // HANDSHAKE NUNCA é deduplicado: forwarda TODA cópia pro WG. Handshake é
                        // raro (rekey ~120s) e perder 1 = stall fixo de 5s no WG (pior em link
                        // móvel/lossy). Deduplicar tiraria a redundância multipath justo aí.
                        // Só transporte (tipo 4) entra no dedup.
                        if (!isHandshake)
                        {
                            uint seq = ((uint)data[1] << 24) | ((uint)data[2] << 16) | ((uint)data[3] << 8) | (uint)data[4];

                            // receiver_index do WG (u32 little-endian no offset 4 do pacote WG,
                            // que comeca apos o header seq) identifica a sessao/cliente. Dedup
                            // por sessao evita colisao de seq entre clientes distintos.
                            uint session = 0;
                            int wgOff = SEQ_HEADER_SIZE;
                            if (len >= wgOff + 8)
                                session = (uint)data[wgOff + 4]
                                        | ((uint)data[wgOff + 5] << 8)
                                        | ((uint)data[wgOff + 6] << 16)
                                        | ((uint)data[wgOff + 7] << 24);

                            bool isDup;
                            lock (_recvSeqLock)
                            {
                                if (!_seqWindows.TryGetValue(session, out var w))
                                {
                                    w = new SeqWindow();
                                    _seqWindows[session] = w;
                                }
                                w.LastSeen = DateTime.UtcNow;

                                // 2-bucket: consulta os dois, insere no current.
                                if (w.Current.Contains(seq) || w.Previous.Contains(seq))
                                {
                                    isDup = true;
                                }
                                else
                                {
                                    isDup = false;
                                    w.Current.Add(seq);
                                    // Rotacao: current cheio -> vira previous, novo current vazio.
                                    // Janela efetiva 4096-8192 seqs (~64-128s a 64pps), sem flush
                                    // total -> sem janela de race apos o cleanup.
                                    if (w.Current.Count > RECV_SEQ_WINDOW)
                                    {
                                        w.Previous = w.Current;
                                        w.Current = new HashSet<uint>();
                                    }
                                }
                            }

                            if (isDup)
                            {
                                Interlocked.Increment(ref _totalSeqDeduped);
                                return; // descarta duplicata, não forwarda
                            }
                            Interlocked.Increment(ref _totalSeqPassed);
                        }

                        // Strip do header de 6 bytes vira só um offset — o SendTo manda data[6..].
                        sendOff = SEQ_HEADER_SIZE;
                    }
                    else if (_seqEnabled)
                    {
                        Interlocked.Increment(ref _totalLegacy);
                    }

            // FORWARD INLINE pro DEST via SendTo zero-alloc (Span + SocketAddress cacheado):
            // sem new byte[] do strip, sem serializar o IPEndPoint a cada pacote. sendOff
            // pula o header seq (6) quando presente, senão 0.
            bckClient.Client.SendTo(data.AsSpan(sendOff, len - sendOff), SocketFlags.None, _destSockAddr!);
        }

        //Received from Server sent back to MP Client
        private static void BckService()
        {
            Console.WriteLine("[Server] BckService is running...");

            //Initialize routes - Error check for null
            if (routes == null)
                routes = new ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();

            while (true)
            {
                try
                {
                    IPEndPoint _loopback = new IPEndPoint(IPAddress.IPv6Any, 0);
                    byte[] data = bckClient.Receive(ref _loopback);

                    // data[0] = tipo da msg WG (kernel manda cru): 1/2/3 = handshake, 4 = dado.
                    byte wgType = data.Length > 0 ? data[0] : (byte)0;
                    bool isHandshake = wgType >= 1 && wgType <= 3;

                    // HANDSHAKE volta RAW (sem header seq): o cliente, sem o magic, entrega direto
                    // ao WG sem deduplicar → toda cópia (todas as rotas) chega no WG. Redundância
                    // máxima no rekey. Não incrementa _serverSeq (mantém a sequência de dados
                    // contígua p/ o gap-tracking do cliente).
                    byte[]? seqData = null;
                    if (_seqEnabled && !isHandshake)
                    {
                        uint seq = Interlocked.Increment(ref _serverSeq);
                        seqData = new byte[data.Length + SEQ_HEADER_SIZE];
                        seqData[0] = SEQ_MAGIC_HEAD;
                        seqData[1] = (byte)((seq >> 24) & 0xFF);
                        seqData[2] = (byte)((seq >> 16) & 0xFF);
                        seqData[3] = (byte)((seq >> 8) & 0xFF);
                        seqData[4] = (byte)(seq & 0xFF);
                        seqData[5] = SEQ_MAGIC_TAIL;
                        Buffer.BlockCopy(data, 0, seqData, SEQ_HEADER_SIZE, data.Length);
                    }

                    // DEMUX por-cliente: o receiver_index do pacote downstream identifica o player.
                    // type 2 (handshake response) tem o receiver_index no offset 8; type 3/4 no
                    // offset 4. Casa com o WgClientIndex aprendido no INIT -> manda SÓ pras rotas
                    // (paths) DESSE player, em vez de broadcast pra TODAS (O(N) waste). Índice
                    // desconhecido (sessão sem INIT visto) -> fallback broadcast (seguro).
                    int idxOff = (wgType == 2) ? 8 : 4;
                    uint clientIdx = (data.Length >= idxOff + 4)
                        ? ((uint)data[idxOff] | ((uint)data[idxOff + 1] << 8) | ((uint)data[idxOff + 2] << 16) | ((uint)data[idxOff + 3] << 24))
                        : 0;

                    ConcurrentDictionary<Route, BlockingCollection<(byte[], UdpClient?)>>? targets = null;
                    if (clientIdx != 0) _clientIndexRoutes.TryGetValue(clientIdx, out targets);
                    var dest = (targets != null && !targets.IsEmpty) ? targets : routes;

                    //Per-route: routes que falaram seq recebem versão seq-encoded; handshake e legacy recebem raw.
                    if (dest != null)
                        foreach (var route in dest)
                        {
                            byte[] toSend = (_seqEnabled && !isHandshake && route.Key.UsesSeq && seqData != null) ? seqData : data;
                            route.Value.Add((toSend, fwClient));
                        }
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }
    }
}

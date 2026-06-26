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
        private static Dictionary<Route, BlockingCollection<(byte[], UdpClient?)>>? routes = new Dictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();
        private static UdpClient fwClient = Utils.CreateDualStackUdpClient(0);
        private static UdpClient bckClient = Utils.CreateDualStackUdpClient(0);

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
        // 2-bucket rotation pro dedup do uplink: sem Clear() total, que abria janela
        // de race (a 2a copia chegando logo apos o flush era marcada como "nova",
        // forwardada pro WG e descartada pelo anti-replay -> OOO contado como loss no
        // jogo). Mesmo fix aplicado no cliente (MultipathRouterService 2-bucket).
        private static HashSet<uint> _recvSeqCurrent = new HashSet<uint>();
        private static HashSet<uint> _recvSeqPrevious = new HashSet<uint>();
        private static readonly object _recvSeqLock = new object();
        private static long _totalSeqDeduped = 0;
        private static long _totalSeqPassed = 0;
        private static long _totalLegacy = 0;

        public static void StartServer(string port, string destination, bool seqEnabled = false)
        {
            _seqEnabled = seqEnabled;
            Console.WriteLine($"Starting server on port {port}... seqDedup={(seqEnabled ? "ON" : "OFF")}");

            _ = Task.Run(() =>
                    FwService(int.Parse(port), destination)
                );


            _ = Task.Run(() =>
                    BckService()
                );

            if (routes == null)
                routes = new Dictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();

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
                        routes.Remove(r);
                    }
                }
            }
        }

        //Received from MP Client to Server
        private static void FwService(int port, string destination)
        {
            Console.WriteLine("[Server] FwService is running...");

            fwClient = Utils.CreateDualStackUdpClient(port);

            //Route and queue to send packets received from MP Client directly to Server
            BlockingCollection<(byte[], UdpClient?)> queue = new BlockingCollection<(byte[], UdpClient?)>();
            Route? route = Utils.ReadRoute(destination, queue);

            if(route == null)
            {
                Console.WriteLine("Missing destination route.\nUsage: mpsingularity server <PORT> \"1.2.3.4:1234\"");
                Environment.Exit(11);
            }

            //Initialize routes - Error check for null
            if (routes == null)
                routes = new Dictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();

            while (true)
            {
                try
                {
                    // Socket dual-stack precisa IPv6Any aqui; IPv4 chega mapeado e e normalizado pra evitar duplicar Route.
                    IPEndPoint _loopback = new IPEndPoint(IPAddress.IPv6Any, 0);
                    byte[] data = fwClient.Receive(ref _loopback);
                    IPAddress clientAddr = Utils.NormalizeAddress(_loopback.Address);

                    //Any new packet received should be registered to be used a route to send packets back through
                    Route _route = new Route() { IPAddress = clientAddr, Port = _loopback.Port };
                    if (!routes.ContainsKey(_route))
                    {
                        BlockingCollection<(byte[], UdpClient?)>? _queue = new BlockingCollection<(byte[], UdpClient?)>();
                        routes.Add(new Route(_queue, null, fwClient) { IPAddress = clientAddr, Port = _loopback.Port }, _queue);
                    }
                    else
                    {
                        // Qualquer pacote recebido = rota viva. Atualiza LastPing do registro existente
                        // pra nao expirar sob trafego alto enquanto o ping reply (1B) ta na fila.
                        var existing = routes.Keys.FirstOrDefault(r => r.Equals(_route));
                        if (existing != null) existing.LastPing = DateTime.UtcNow;
                    }

                    //Whenever clients enables/disables a route, a packet with 3 bytes is sent to this route letting the route know what state it should take
                    if (data.Length == 3)
                    {
                        Route? r = routes.Keys.FirstOrDefault(r => r.Equals(_route));
                        if (r == null)
                            continue;

                        r.SetRouteActive(data[2]);

                        continue;
                    }

                    //Ping Packets will be bounced back - Wireguard Packets (and most regular packets) will always be larger than 2 byte
                    if (data.Length == 2)
                    {
                        fwClient.Send(data.Take(1).ToArray(), 1, _loopback);
                        continue;
                    }

                    if(data.Length == 1)
                    {
                        Route? r = routes.Keys.FirstOrDefault(r => r.Equals(_route));
                        if (r == null)
                            continue;

                        r.CalculateLatency(data[0]);
                    }

                    // -------- SEQ DEDUP (se ativado e header magic detectado) --------
                    byte[] payload = data;
                    if (_seqEnabled
                        && data.Length >= SEQ_HEADER_SIZE
                        && data[0] == SEQ_MAGIC_HEAD
                        && data[5] == SEQ_MAGIC_TAIL)
                    {
                        // Tipo da msg WG vem logo APÓS o header seq de 6 bytes:
                        // 1=handshake init, 2=handshake response, 3=cookie reply, 4=transport data.
                        byte wgType = data.Length > SEQ_HEADER_SIZE ? data[SEQ_HEADER_SIZE] : (byte)0;
                        bool isHandshake = wgType >= 1 && wgType <= 3;

                        // Marca este route como falando seq → respostas vão com header
                        Route? rt = routes.Keys.FirstOrDefault(r => r.Equals(_route));
                        if (rt != null) rt.UsesSeq = true;

                        // HANDSHAKE NUNCA é deduplicado: forwarda TODA cópia pro WG. Handshake é
                        // raro (rekey ~120s) e perder 1 = stall fixo de 5s no WG (pior em link
                        // móvel/lossy). Deduplicar tiraria a redundância multipath justo aí.
                        // Só transporte (tipo 4) entra no dedup.
                        if (!isHandshake)
                        {
                            uint seq = ((uint)data[1] << 24) | ((uint)data[2] << 16) | ((uint)data[3] << 8) | (uint)data[4];
                            bool isDup;
                            lock (_recvSeqLock)
                            {
                                // 2-bucket: consulta os dois, insere no current.
                                if (_recvSeqCurrent.Contains(seq) || _recvSeqPrevious.Contains(seq))
                                {
                                    isDup = true;
                                }
                                else
                                {
                                    isDup = false;
                                    _recvSeqCurrent.Add(seq);
                                    // Rotacao: current cheio -> vira previous, novo current vazio.
                                    // Janela efetiva 4096-8192 seqs (~64-128s a 64pps), sem flush
                                    // total -> sem janela de race apos o cleanup.
                                    if (_recvSeqCurrent.Count > RECV_SEQ_WINDOW)
                                    {
                                        _recvSeqPrevious = _recvSeqCurrent;
                                        _recvSeqCurrent = new HashSet<uint>();
                                    }
                                }
                            }

                            if (isDup)
                            {
                                Interlocked.Increment(ref _totalSeqDeduped);
                                continue; // descarta duplicata, não forwarda
                            }
                            Interlocked.Increment(ref _totalSeqPassed);
                        }

                        // Strip 6-byte header antes de entregar ao WG kernel (handshake e dado)
                        payload = new byte[data.Length - SEQ_HEADER_SIZE];
                        Buffer.BlockCopy(data, SEQ_HEADER_SIZE, payload, 0, payload.Length);
                    }
                    else if (_seqEnabled)
                    {
                        Interlocked.Increment(ref _totalLegacy);
                    }

                    //Use Destination queue to finish delivery of packet to Server
                    queue.Add((payload, bckClient));
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        //Received from Server sent back to MP Client
        private static void BckService()
        {
            Console.WriteLine("[Server] BckService is running...");

            //Initialize routes - Error check for null
            if (routes == null)
                routes = new Dictionary<Route, BlockingCollection<(byte[], UdpClient?)>>();

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

                    //Send packet back through all routes that have connected to server.
                    //Per-route: routes que falaram seq recebem versão seq-encoded; handshake e legacy recebem raw.
                    foreach (var route in routes)
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

using TisaiMultipath.Helpers;
using TisaiMultipath.Services;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TisaiMultipath.Models
{
    public class Route
    {
        // Ping a cada 500ms — 3x mais probes que o original (1500ms).
        // Sob 30% loss, P(2 perdidos seguidos)=0.09 vs 0.3 do ping 1500ms; rota sobrevive a microblips.
        public const int PING_INTERVAL_MS = 500;

        // Renovacao do socket so se REALMENTE morto (30s sem qualquer recv).
        public const double SOCKET_RENEWAL_TIMEOUT_S = 30;

        public Route()
        {
        }

        public Route(BlockingCollection<(byte[], UdpClient?)>? queue, BlockingCollection<byte[]>? bckQueue = null, UdpClient? udpClient = null)
        {
            // LongRunning = thread DEDICADA por rota fora do ThreadPool (não Task.Run).
            // WorkerThread bloqueia em queue.Take() pra sempre. Em ThreadPool, N rotas =
            // N workers presos -> o pool injeta mais (throttle ~1/seg) -> starvation:
            // rajada de rotas novas (CS2 conectando + churn) engasga TODAS as rotas do
            // processo juntas. LongRunning tira do pool (sem starvation) e mantém semântica
            // de Task (exceção não derruba o processo, diferente de new Thread).
            if (queue != null)
                Task.Factory.StartNew(() => WorkerThread(queue), TaskCreationOptions.LongRunning);

            if (bckQueue != null)
            {
                listenerTask = Task.Run(() => UdpThread(bckQueue));
                _bckQueue = bckQueue;
            }

            //Can route be replaced.
            canRenewRoute = udpClient == null;

            //Save UDP Client to activate/deactivate routes
            _udpClient = udpClient;

            //If any task is running, start latency thread
            if (bckQueue != null || queue != null)
            {
                if (udpClient != null)
                    Task.Factory.StartNew(() => LatencyThread(ref udpClient), TaskCreationOptions.LongRunning);
                else
                    Task.Factory.StartNew(() => LatencyThread(ref _routeUdp), TaskCreationOptions.LongRunning);
            }

        }

        public IPAddress IPAddress { get; set; } = new IPAddress(0);
        public int Port { get; set; }
        public double Latency { get; set; } = 999;
        public DateTime LastPing { get; set; } = DateTime.UtcNow;

        private byte latencyIdx = 0;
        private DateTime latencyStart = DateTime.UtcNow;

        private UdpClient _routeUdp = Utils.CreateDualStackUdpClient(0);
        private UdpClient _udpClient;
        private bool canRenewRoute = false;
        private Task listenerTask;
        private BlockingCollection<byte[]>? _bckQueue;

        public bool active { get; set; } = true;

        /// <summary>
        /// Cliente neste route fala protocolo seq (header 0xAA+seq4+0x55). Marcado true
        /// na 1a incoming com magic detectado. Server usa pra decidir se adiciona seq
        /// header na resposta — clientes legacy continuam recebendo bytes raw.
        /// </summary>
        public bool UsesSeq { get; set; } = false;

        /// <summary>
        /// Índice WG do CLIENTE desta sessão (sender_index do handshake INIT). Usado pelo
        /// demux do downstream: o BckService manda a resposta só pras rotas com esse índice
        /// (o WG carimba ele como receiver_index no downstream), em vez de broadcast pra
        /// TODAS as rotas. 0 = ainda não aprendido (INIT não visto) -> fallback broadcast.
        /// </summary>
        public uint WgClientIndex { get; set; } = 0;

        private readonly object _udpLock = new object();

        private void WorkerThread(BlockingCollection<(byte[], UdpClient?)> queue)
        {
            if (IPAddress == null)
                return;

            while (true)
            {
                if (!active)
                {
                    Thread.Sleep(100);
                    continue;
                }


                // Wait for data to become available
                var (data, udp) = queue.Take();

                lock (_udpLock)
                {
                    //Send through specified client or through route udp
                    (udp ?? _routeUdp).Send(data, data.Length, new IPEndPoint(IPAddress, Port));
                }
            }
        }

        private void UdpThread(BlockingCollection<byte[]> bckQueue)
        {
            while (true)
            {
                // Socket dual-stack precisa de IPv6Any aqui — IPv4 chega como ::ffff:1.2.3.4 mapeado.
                IPEndPoint _loopback = new IPEndPoint(IPAddress.IPv6Any, 0);
                byte[] data = _routeUdp.Receive(ref _loopback);

                // Qualquer pacote recebido = rota viva. Sob trafego alto o ping (1B) entra
                // na fila atras dos WG e LastPing fica velha → CalculateDynamicRoutes desativava
                // a rota mesmo com dados fluindo. Agora qualquer recv refresca o heartbeat.
                LastPing = DateTime.UtcNow;

                //Ping Packets will be bounced back - Wireguard Packets (and most regular packets) will always be larger than 2 byte
                if (data.Length == 2)
                {
                    _routeUdp.Send(data.Take(1).ToArray(), 1, _loopback);
                    continue;
                }

                if (data.Length == 1)
                {
                    CalculateLatency(data[0]);
                    continue;
                }

                bckQueue.Add(data);
            }
        }

        public void SetActive(bool _active)
        {
            active = _active;


            switch (active)
            {
                case true:
                    _routeUdp.Send(new byte[] { 0, 0, 1 }, 3, new IPEndPoint(IPAddress, Port));
                    Console.WriteLine($"Disabling route: {IPAddress}:{Port}");
                    break;
                case false:
                    _routeUdp.Send(new byte[] { 0, 0, 0 }, 3, new IPEndPoint(IPAddress, Port));
                    Console.WriteLine($"Enabling route: {IPAddress}:{Port}");
                    break;
            }
        }

        public void SetRouteActive(byte a)
        {
            // Control packet de 3 bytes e reenviado pelo client como keepalive (~1.5s/rota,
            // x copias multipath). IDEMPOTENTE: so age/loga quando o estado MUDA. Sem isso,
            // o server "habilitava" uma rota ja ativa dezenas de vezes/s — log spam +
            // scan/thrash na thread unica do FwService (mesma que processa handshake).
            if (a != 0 && a != 1) return;          // fora do protocolo
            bool newActive = a == 1;
            if (active == newActive) return;       // sem mudanca -> no-op

            active = newActive;
            Console.WriteLine(newActive
                ? $"Enabling route: {IPAddress}:{Port}"
                : $"Disabling route: {IPAddress}:{Port}");
        }

        private void LatencyThread(ref UdpClient client)
        {
            while (true)
            {
                client.Send(new byte[] { ++latencyIdx, 0 }, 2, new IPEndPoint(IPAddress, Port));

                if (_udpClient is null) //If running as a client
                    client.Send(new byte[] { 0, 0, (active ? (byte)1 : (byte)0) }, 3, new IPEndPoint(IPAddress, Port)); //Keeps route state synchronized on server


                latencyStart = DateTime.UtcNow;

                Thread.Sleep(PING_INTERVAL_MS);

                if (canRenewRoute && (DateTime.UtcNow - LastPing).TotalSeconds > SOCKET_RENEWAL_TIMEOUT_S)
                {
                    lock (_udpLock)
                    {
                        client = Utils.CreateDualStackUdpClient(0);

                        if (_bckQueue != null)
                        {
                            listenerTask.Dispose();
                            listenerTask = Task.Run(() => UdpThread(_bckQueue));
                        }

                        LastPing = DateTime.UtcNow;
                    }
                }
            }
        }

        // Recebeu pong (1B). Sempre atualiza LastPing (mantem rota viva mesmo se pong fora
        // de ordem). Latency suavizada via EWMA pra nao saltar pra 999 em uma perda singular.
        public void CalculateLatency(byte idx)
        {
            LastPing = DateTime.UtcNow;

            if (idx != latencyIdx)
                return; // pong de probe antigo — heartbeat conta, mas nao usa pra calc RTT

            double sample = (DateTime.UtcNow - latencyStart).TotalMilliseconds;

            // Primeiro sample inicializa direto. Depois EWMA 70/30 — picos suavizam,
            // perdas pontuais nao destroem a metrica.
            Latency = (Latency >= 999) ? sample : (Latency * 0.7 + sample * 0.3);
        }

        //The following 2 methods (Equals and GetHashCode) ensure that this class can be compared using IPAddress and Port only ignoring everything else.
        public override bool Equals(object? obj)
        {
            if (obj == null || GetType() != obj.GetType())
                return false;

            Route other = (Route)obj;
            return IPAddress.Equals(other.IPAddress) && Port == other.Port;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + (IPAddress?.GetHashCode() ?? 0);
            hash = hash * 23 + Port.GetHashCode();
            return hash;
        }
    }
}

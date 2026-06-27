using TisaiMultipath.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TisaiMultipath.Helpers
{
    public static class Utils
    {
        // Cria UdpClient AF_INET6 com DualMode (recebe e envia IPv4 e IPv6) + buffers grandes.
        // Buffers de 8MB absorvem rajadas WG sem o kernel dropar pings que ficaram atras na fila.
        public static UdpClient CreateDualStackUdpClient(int port)
        {
            var socket = new Socket(AddressFamily.InterNetworkV6, SocketType.Dgram, ProtocolType.Udp);
            try { socket.DualMode = true; } catch { /* SO pode nao suportar — segue v6 puro */ }
            socket.Bind(new IPEndPoint(IPAddress.IPv6Any, port));
            var client = new UdpClient { Client = socket };
            try
            {
                client.Client.ReceiveBufferSize = 8 * 1024 * 1024;
                client.Client.SendBufferSize = 8 * 1024 * 1024;
            }
            catch { }
            return client;
        }

        // Igual ao CreateDualStackUdpClient mas com SO_REUSEPORT — permite VÁRIAS threads
        // bindarem a MESMA porta; o kernel balanceia os pacotes por fluxo (4-tupla src/dst)
        // entre os sockets. Cada cliente (4-tupla fixa) cai SEMPRE no mesmo socket/thread,
        // então o estado por-cliente continua single-thread (sem race). Destrava o FwService
        // de "1 core fixo" pra "escala com nº de cores". Linux: SOL_SOCKET=1, SO_REUSEPORT=15
        // — tem que ser setado ANTES do Bind.
        public static UdpClient CreateReusePortUdpClient(int port)
        {
            var socket = new Socket(AddressFamily.InterNetworkV6, SocketType.Dgram, ProtocolType.Udp);
            try { socket.DualMode = true; } catch { }
            try { socket.SetRawSocketOption(1, 15, BitConverter.GetBytes(1)); } catch { }
            socket.Bind(new IPEndPoint(IPAddress.IPv6Any, port));
            var client = new UdpClient { Client = socket };
            try
            {
                client.Client.ReceiveBufferSize = 16 * 1024 * 1024;
                client.Client.SendBufferSize = 16 * 1024 * 1024;
            }
            catch { }
            return client;
        }

        // Normaliza endereco IPv4-mapped (::ffff:1.2.3.4) -> 1.2.3.4 puro.
        // Sem isso o Dictionary de Routes duplica entrada quando o cliente fala IPv4 num socket dual-stack.
        public static IPAddress NormalizeAddress(IPAddress addr)
        {
            return (addr.IsIPv4MappedToIPv6) ? addr.MapToIPv4() : addr;
        }

        public static Route? ReadRoute(string route, BlockingCollection<(byte[], UdpClient?)>? queue, BlockingCollection<byte[]>? _bckQueue = null)
        {
            // IPEndPoint.TryParse cobre dotted-quad ("1.2.3.4:1234") E IPv6 com brackets ("[::1]:1234").
            if (!IPEndPoint.TryParse(route.Trim(), out IPEndPoint? endpoint) || endpoint == null)
            {
                Console.WriteLine($"Route: {route} [Invalid Endpoint — use 1.2.3.4:1234 ou [v6]:1234]");
                return null;
            }

            if (endpoint.Port < 0 || endpoint.Port > 65535)
            {
                Console.WriteLine($"Route: {route} [Invalid Port]");
                return null;
            }

            if (queue == null)
                return null;

            return new Route(queue, _bckQueue)
            {
                IPAddress = NormalizeAddress(endpoint.Address),
                Port = endpoint.Port
            };
        }

        public static void PrintRouteStates(List<Route> routes, bool clear)
        {
            try
            {
                if (clear)
                    Console.Clear();
            }
            catch { }

            Console.WriteLine($"Routes: {routes.Count}");
            foreach (Route r in routes)
            {
                Console.WriteLine($"{r.IPAddress}:{r.Port} - [{r.Latency:0.00}] [{r.LastPing:HH:mm:ss}] [Active: {r.active}]");
            }
        }
    }
}

using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace TisaiMultipath.Helpers
{
    /// <summary>
    /// Receive em LOTE via recvmmsg (Linux): pega até N datagramas em UMA syscall, em vez
    /// de uma syscall por pacote. Reduz o custo de viagem user↔kernel (~1µs/pacote) que
    /// domina em pps alto. Estruturas nativas (buffers/iovecs/sockaddrs/mmsghdrs) são
    /// alocadas UMA vez e reusadas (sem alloc por chamada). Linux x86-64.
    /// </summary>
    internal sealed class BatchReceiver : IDisposable
    {
        const int MAXPKT = 2048;
        // tamanhos x86-64 Linux
        const int SZ_SOCKADDR = 28;   // sockaddr_in6
        const int SZ_IOVEC = 16;      // {void* base; size_t len}
        const int SZ_MSGHDR = 56;     // {name*,namelen,pad,iov*,iovlen,control*,controllen,flags,pad}
        const int SZ_MMSGHDR = 64;    // msghdr(56) + msg_len(4) + pad(4)

        readonly int _fd;
        readonly int _n;
        readonly IntPtr _data, _addrs, _iovecs, _mmsgs;
        readonly byte[] _rxbuf = new byte[MAXPKT]; // buffer reusado p/ handler (zero-alloc; síncrono)

        [DllImport("libc", SetLastError = true)]
        static extern int recvmmsg(int sockfd, IntPtr msgvec, uint vlen, int flags, IntPtr timeout);

        [DllImport("libc", SetLastError = true)]
        static extern int fcntl(int fd, int cmd, int arg);

        public BatchReceiver(Socket sock, int batch)
        {
            _fd = (int)sock.Handle;
            _n = batch;
            // .NET Core deixa o fd NON-BLOCKING (usa epoll interno). recvmmsg num fd
            // non-blocking retorna EAGAIN na hora -> busy-spin sem pegar pacote. Força
            // BLOCKING via fcntl (só usamos recvmmsg + Send neste socket no modo batch).
            const int F_GETFL = 3, F_SETFL = 4, O_NONBLOCK = 0x800;
            int fl = fcntl(_fd, F_GETFL, 0);
            if (fl >= 0) fcntl(_fd, F_SETFL, fl & ~O_NONBLOCK);
            _data   = Marshal.AllocHGlobal(MAXPKT * _n);
            _addrs  = Marshal.AllocHGlobal(SZ_SOCKADDR * _n);
            _iovecs = Marshal.AllocHGlobal(SZ_IOVEC * _n);
            _mmsgs  = Marshal.AllocHGlobal(SZ_MMSGHDR * _n);
            for (int i = 0; i < _n; i++)
            {
                IntPtr buf  = _data  + MAXPKT * i;
                IntPtr addr = _addrs + SZ_SOCKADDR * i;
                IntPtr iov  = _iovecs + SZ_IOVEC * i;
                IntPtr mh   = _mmsgs + SZ_MMSGHDR * i;
                // iovec = {buf, MAXPKT}
                Marshal.WriteIntPtr(iov, 0, buf);
                Marshal.WriteIntPtr(iov, 8, (IntPtr)MAXPKT);
                // msghdr: name=addr, namelen=SZ_SOCKADDR, iov=iov, iovlen=1, control=0, controllen=0, flags=0
                Marshal.WriteIntPtr(mh, 0, addr);          // msg_name
                Marshal.WriteInt32(mh, 8, SZ_SOCKADDR);    // msg_namelen (+4 pad)
                Marshal.WriteIntPtr(mh, 16, iov);          // msg_iov
                Marshal.WriteIntPtr(mh, 24, (IntPtr)1);    // msg_iovlen
                Marshal.WriteIntPtr(mh, 32, IntPtr.Zero);  // msg_control
                Marshal.WriteIntPtr(mh, 40, IntPtr.Zero);  // msg_controllen
                Marshal.WriteInt32(mh, 48, 0);             // msg_flags (+4 pad)
                // mmsghdr.msg_len em offset 56
                Marshal.WriteInt32(mh, 56, 0);
            }
        }

        /// <summary>
        /// Bloqueia até receber 1..N datagramas. Para cada i &lt; retorno, chama handler com
        /// o buffer (cópia segura), tamanho, e o IPEndPoint de origem (normalizado).
        /// Retorna o nº de datagramas, ou -1 em erro.
        /// </summary>
        public int Receive(Action<byte[], int, IPAddress, int> handler)
        {
            // reseta namelen e msg_len antes de cada chamada (recvmmsg sobrescreve)
            for (int i = 0; i < _n; i++)
            {
                IntPtr mh = _mmsgs + SZ_MMSGHDR * i;
                Marshal.WriteInt32(mh, 8, SZ_SOCKADDR);
            }
            int got = recvmmsg(_fd, _mmsgs, (uint)_n, 0, IntPtr.Zero);
            if (got <= 0) return got;
            for (int i = 0; i < got; i++)
            {
                IntPtr mh = _mmsgs + SZ_MMSGHDR * i;
                int len = Marshal.ReadInt32(mh, 56);
                if (len <= 0 || len > MAXPKT) continue;
                IntPtr buf = _data + MAXPKT * i;
                Marshal.Copy(buf, _rxbuf, 0, len);   // copia pro buffer REUSADO (sem new byte[])
                // sockaddr_in6: family(2) port(2 BE) flowinfo(4) addr(16) scope(4)
                IntPtr addr = _addrs + SZ_SOCKADDR * i;
                int port = (Marshal.ReadByte(addr, 2) << 8) | Marshal.ReadByte(addr, 3);
                byte[] a16 = new byte[16];
                Marshal.Copy(addr + 8, a16, 0, 16);
                // passa o IP CRU (v6-mapped p/ clientes v4); quem processa normaliza p/ a
                // chave da Route mas usa o cru pro bounce (send em socket dual-stack).
                handler(_rxbuf, len, new IPAddress(a16), port);
            }
            return got;
        }

        public void Dispose()
        {
            Marshal.FreeHGlobal(_data); Marshal.FreeHGlobal(_addrs);
            Marshal.FreeHGlobal(_iovecs); Marshal.FreeHGlobal(_mmsgs);
        }
    }
}

using Microsoft.ML.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S3Parser
{
    class PacketReport
    {
        [LoadColumn(0)]
        public long GatewayTimestamp { get; set; }
        [LoadColumn(1)]
        public ulong OUI { get; set; }
        [LoadColumn(2)]
        public ulong NetID { get; set; }
        [LoadColumn(3)]
        public int RSSI { get; set; }
        [LoadColumn(4)]
        public uint Frequency { get; set; }
        [LoadColumn(5)]
        public float SNR { get; set; }
        [LoadColumn(6)]
        public int DataRate { get; set; }
        [LoadColumn(7)]
        public string? Region { get; set; }
        [LoadColumn(8)]
        public string? Gateway { get; set; }
        [LoadColumn(9)]
        public string? PayloadHash { get; set; }
        [LoadColumn(10)]
        public uint PayloadSize { get; set; }
    };
}

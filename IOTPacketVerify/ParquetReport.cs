
using System.Xml.Linq;

struct ParquetReport
{
    public ulong PacketTimestamp { get; set; }

    public string? Gateway { get; set; }

    public string? PayloadHash { get; set; }

    public uint PayloadSize { get; set; }

    public uint NumDCs { get; set; }
};

public struct PacketSummary
{
    public DateTime ModTime;
    public ulong MessageCount;
    public ulong DupeCount;
    public ulong TotalBytes;
    public ulong DCCount;
    public ulong FileCount;
    public ulong RawSize;
    public ulong GzipSize;

    public override String ToString()
    {
        return $"{MessageCount} {DupeCount} {TotalBytes} {DCCount} {FileCount} {RawSize} {GzipSize}";
    }
};

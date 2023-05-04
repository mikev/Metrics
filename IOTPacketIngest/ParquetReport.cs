
struct ParquetReport
{
    //[MapToColumn("GatewayTimestamp")]
    public ulong GatewayTimestamp { get; set; }

    //[MapToColumn("OUI")]
    public ulong OUI { get; set; }

    //[MapToColumn("NetID")]
    public ulong NetID { get; set; }
    //[MapToColumn("RSSI")]
    public int RSSI { get; set; }

    //[MapToColumn("Frequency")]
    public uint Frequency { get; set; }

    //[MapToColumn("SNR")]
    public float SNR { get; set; }

    //[MapToColumn("DataRate")]
    public int DataRate { get; set; }

    //[MapToColumn("Region")]
    public string? Region { get; set; }

    //[MapToColumn("Gateway")]
    public string? Gateway { get; set; }

    //[MapToColumn("PayloadHash")]
    public string? PayloadHash { get; set; }

    //[MapToColumn("PayloadSize")]
    public uint PayloadSize { get; set; }
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


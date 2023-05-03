
struct ParquetReport
{
    public ulong PacketTimestamp { get; set; }

    public string? Gateway { get; set; }

    public string? PayloadHash { get; set; }

    public uint PayloadSize { get; set; }

    public uint NumDCs { get; set; }
};

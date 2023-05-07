struct Data
{
    public ulong PacketTimestamp { get; set; }

    public string? Gateway { get; set; }

    public string? PayloadHash { get; set; }

    public uint PayloadSize { get; set; }

    public uint NumDCs { get; set; }
};

public class PacketSummary
{
    public int IDHash { get; set; }
    public DateTime Time { get; set; }
    public uint Duration { get; set; }
    public ulong DCCount { get; set; }
    public ulong PacketCount { get; set; }
    public ulong DupeCount { get; set; }
    public ulong PacketBytes { get; set; }
    public ulong Files { get; set; }
    public ulong RawBytes { get; set; }
    public ulong GzipBytes { get; set; }
    public override String ToString()
    {
        return $"{PacketCount} {DupeCount} {PacketBytes} {DCCount} {Files} {RawBytes} {GzipBytes}";
    }
    public override int GetHashCode()
    {
        return Time.GetHashCode();
        //unchecked // Allow arithmetic overflow, numbers will just "wrap around"
        //{
        //    int hashcode = 1430287;
        //    hashcode = hashcode * 7302013 ^ Time.GetHashCode();
        //    hashcode = hashcode * 7302013 ^ DCCount.GetHashCode();
        //    hashcode = hashcode * 7302013 ^ PacketCount.GetHashCode();
        //    hashcode = hashcode * 7302013 ^ DupeCount.GetHashCode();
        //    hashcode = hashcode * 7302013 ^ PacketBytes.GetHashCode();
        //    hashcode = hashcode * 7302013 ^ Files.GetHashCode();
        //    hashcode = hashcode * 7302013 ^ RawBytes.GetHashCode();
        //    hashcode = hashcode * 7302013 ^ GzipBytes.GetHashCode();
        //    return hashcode;
        //}
    }
}

public class OUISummary
{
    public uint IDHash { get; set; }
    public DateTime Time { get; set; }
    public uint OUI { get; set;}
    public ulong DCCount { get; set; }
    public float Percent { get; set; }
}

public class RegionSummary
{
    public uint IDHash { get; set; }
    public DateTime Time { get; set; }
    public uint Region { get; set; }
    public ulong DCCount { get; set; }
    public float Percent { get; set; }
}

public class LoRaWANMetrics
{
    public HashSet<PacketSummary>? VerifyByDay { get; set; }
    public HashSet<PacketSummary>? IngestByDay { get; set; }
    public HashSet<OUISummary>? OUIByDay { get; set; }
    public HashSet<RegionSummary>? RegionByDay { get; set; }
};

public struct ReportSummary
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

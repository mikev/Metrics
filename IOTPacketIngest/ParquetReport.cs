
using CsvHelper.Configuration.Attributes;
using Google.Protobuf;
using System.Numerics;

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

public class IOTMetadata
{
    [Index(0)]
    public string HotspotKey { get; set; }

    [Index(1)]
    public BigInteger? Location { get; set; }

    [Index(2)]
    public int Elevation { get; set; }

    [Index(3)]
    public uint Gain { get; set; }

    [Index(4)]
    public char IsFullHotspot { get; set; }

    [Index(5)]
    public string Created { get; set; }

    [Index(6)]
    public string Updated { get; set; }
}

public static class XX
{
    public static int ToIntHash(this ByteString ob)
    {
        return BitConverter.ToInt32(ob.ToByteArray(), 0);
    }
};

public class PacketSummary
{
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
        var hashCode = Time.GetHashCode();
        return hashCode;
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

public class RedundantSummary
{
    public DateTime Time { get; set; }
    public float Percent { get; set; }
    public uint Region { get; set; }
}

public class OUISummary
{
    public DateTime Time { get; set; }
    public uint OUI { get; set; }
    public ulong DCCount { get; set; }
    public float Percent { get; set; }

    public override int GetHashCode()
    {
        var hashCode = Time.GetHashCode();
        return hashCode;
    }

}

public class RegionSummary
{
    public DateTime Time { get; set; }
    public uint Region { get; set; }
    public ulong DCCount { get; set; }
    public float Percent { get; set; }

    public override int GetHashCode()
    {
        var hashCode = Time.GetHashCode();
        return hashCode;
    }
}

public class NetIDSummary
{
    public DateTime Time { get; set; }
    public uint NetID { get; set; }
    public ulong DCCount { get; set; }
    public float Percent { get; set; }

    public override int GetHashCode()
    {
        var hashCode = Time.GetHashCode();
        return hashCode;
    }
}

public class LoRaWANMetrics
{
    public DateTime LastUpdate { get; set; }
    public List<PacketSummary>? VerifyByDay { get; set; }
    public List<PacketSummary>? IngestByDay { get; set; }
    public List<OUISummary>? OUIByDay { get; set; }
    public List<RegionSummary>? RegionByDay { get; set; }
    public List<NetIDSummary>? NetIDByDay { get; set; }
    public List<RedundantSummary>? RedundantByDay { get; set; }
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

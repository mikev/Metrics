
using Google.Protobuf;
using Helium;
using Helium.PacketRouter;
using Parquet.Schema;
using System;
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

public static class XX
{
    public static int ToIntHash(this ByteString ob)
    {
        return BitConverter.ToInt32(ob.ToByteArray(), 0);
    }
};

public class ParserBase
{
    public List<Array> GetArray()
    {
        return new List<Array>();
    }

    public ParquetSchema GetSchema()
    {
        return new ParquetSchema();
    }

    public void ParseMessagePacketReport(byte[] message)
    {
    }
}

public class IngestParser : ParserBase
{
    public ParquetData ParquetData { get; set; }

    public IngestParser()
    {
        ParquetData = new ParquetData();
    }

    public List<Array> GetArray()
    {
        List<Array> parquetArray = new List<Array>();
        if (ParquetData is null)
            return parquetArray;

        var parquetData = ParquetData;
        parquetArray?.Add(parquetData.GatewayTimestampMSList.ToArray());
        parquetArray?.Add(parquetData?.OUIList?.ToArray());
        parquetArray?.Add(parquetData?.NetIDList?.ToArray());
        parquetArray?.Add(parquetData?.RSSIList?.ToArray());
        parquetArray?.Add(parquetData?.FrequencyList?.ToArray());
        parquetArray?.Add(parquetData?.SNRList?.ToArray());
        parquetArray?.Add(parquetData?.DataRateList?.ToArray());
        parquetArray?.Add(parquetData?.RegionList?.ToArray());
        parquetArray?.Add(parquetData?.GatewayList?.ToArray());
        parquetArray?.Add(parquetData?.PayloadHashList?.ToArray());
        parquetArray?.Add(parquetData?.PayloadSizeList?.ToArray());
        parquetArray?.Add(parquetData?.FreeList?.ToArray());
        return parquetArray;
    }

    public ParquetSchema GetSchema()
    {
        var schema = new ParquetSchema(
            new DataField<ulong>("gateway_timestamp_ms"),
            new DataField<ulong>("oui"),
            new DataField<uint>("net_id"),
            new DataField<int>("rssi"),
            new DataField<uint>("frequency"),
            new DataField<float>("snr"),
            new DataField<ushort>("data_rate"),
            new DataField<ushort>("region"),
            new DataField<Byte[]>("gateway"),
            new DataField<Byte[]>("payload_hash"),
            new DataField<uint>("payload_size"),
            new DataField<bool>("free")
        );
        return schema;
    }

    public void ParseMessagePacketReport(byte[] message)
    {
        var parquetData = ParquetData;
        var mData = packet_router_packet_report_v1.Parser.ParseFrom(message);
        parquetData?.GatewayTimestampMSList?.Add(mData.GatewayTimestampMs);
        parquetData?.OUIList?.Add(mData.Oui);
        parquetData?.NetIDList?.Add(mData.NetId);
        parquetData?.RSSIList?.Add(mData.Rssi);
        parquetData?.FrequencyList?.Add(mData.Frequency);
        parquetData?.SNRList?.Add(mData.Snr);
        parquetData?.DataRateList?.Add((ushort)mData.Datarate);
        parquetData?.RegionList?.Add((ushort)mData.Region);
        parquetData?.GatewayList?.Add(mData.Gateway.ToByteArray());
        parquetData?.PayloadHashList?.Add(mData.PayloadHash.ToByteArray());
        parquetData?.PayloadSizeList?.Add(mData.PayloadSize);
        parquetData?.FreeList?.Add(mData.Free);
    }
}



public class ParquetData
{
    public List<ulong>? GatewayTimestampMSList { get; set; }
    public List<ulong>? OUIList { get; set; }
    public List<uint>? NetIDList { get; set; }
    // signal strength in dBm
    public List<int>? RSSIList { get; set; }
    // Frequency in hz
    public List<uint>? FrequencyList { get; set; }
    public List<float>? SNRList { get; set; }
    public List<ushort>? DataRateList { get; set; }
    public List<ushort>? RegionList { get; set; }
    
    public List<Byte[]>? GatewayList { get; set; }
    // Hash of `payload` within `message packet`
    public List<Byte[]>? PayloadHashList { get; set; }
    public List<uint>? PayloadSizeList { get; set; }
    public List<bool>? FreeList { get; set; }

    public ParquetData()
    {
        GatewayTimestampMSList = new List<UInt64>();
        OUIList = new List<ulong>();
        NetIDList = new List<UInt32>();
        RSSIList = new List<int>();
        FrequencyList = new List<uint>();
        SNRList = new List<float>();
        DataRateList = new List<ushort>();
        RegionList = new List<ushort>();
        GatewayList = new List<Byte[]>();
        PayloadHashList = new List<Byte[]>();
        PayloadSizeList = new List<uint>();
        FreeList = new List<bool>();
    }
}

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

public class LoRaWANMetrics
{
    public DateTime LastUpdate { get; set; }
    public List<PacketSummary>? VerifyByDay { get; set; }
    public List<PacketSummary>? IngestByDay { get; set; }
    public List<OUISummary>? OUIByDay { get; set; }
    public List<RegionSummary>? RegionByDay { get; set; }
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

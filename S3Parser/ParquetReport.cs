

using ParquetSharp.RowOriented;

struct ParquetReport
{
    [MapToColumn("GatewayTimestamp")]
    public long GatewayTimestamp;

    [MapToColumn("OUI")]
    public ulong OUI;

    [MapToColumn("NetID")]
    public ulong NetID;

    [MapToColumn("RSSI")]
    public int RSSI;

    [MapToColumn("Frequency")]
    public uint Frequency;

    [MapToColumn("SNR")]
    public float SNR;

    [MapToColumn("DataRate")]
    public int DataRate;

    [MapToColumn("Region")]
    public string Region;

    [MapToColumn("Gateway")]
    public string Gateway;

    [MapToColumn("PayloadHash")]
    public string PayloadHash;

    [MapToColumn("PayloadSize")]
    public uint PayloadSize;
};

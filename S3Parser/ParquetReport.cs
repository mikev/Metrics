

using ParquetSharp.RowOriented;

struct ParquetReport
{
    [MapToColumn("GatewayTimestamp")]
    public long GatewayTimestamp;

    [MapToColumn("OUI")]
    public long OUI;

    [MapToColumn("NetID")]
    public long NetID;

    [MapToColumn("RSSI")]
    public int RSSI;

    [MapToColumn("Frequency")]
    public int Frequency;

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
    public int PayloadSize;
};

using ParquetSharp.RowOriented;

struct ParquetGatewayReward
{
    [MapToColumn("HotspotKey")]
    public string HotspotKey;

    [MapToColumn("BeaconAmount")]
    public ulong BeaconAmount;

    [MapToColumn("WitnessAmount")]
    public ulong WitnessAmount;

    [MapToColumn("StartPeriod")]
    public ulong StartPeriod;

    [MapToColumn("EndPeriod")]
    public ulong EndPeriod;
};
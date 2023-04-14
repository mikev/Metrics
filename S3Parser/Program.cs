// See https://aka.ms/new-console-template for more information
using Google.Protobuf;
using Helium.PacketRouter;
using ParquetSharp.RowOriented;
using System.IO.Compression;
using System.Collections.Immutable;

// To interact with Amazon S3.
using Amazon.S3;
using Amazon.Runtime;
using Helium.PocLora;
using Amazon.S3.Model;
using static System.Net.Mime.MediaTypeNames;
using System.IO;
using System.Runtime.CompilerServices;

Console.WriteLine("Hello, World!");


// Create an S3 client object.
var s3Client = new AmazonS3Client();

var list = ListBucketContentsAsync(s3Client, "foundation-iot-packet-ingest", "packetreport.1681334821566");
await foreach( var item in list)
{
    Console.WriteLine($"key {item}");
}

var listResponse = await s3Client.ListBucketsAsync();
foreach (var b in listResponse.Buckets)
{
    Console.WriteLine($"Bucket {b.BucketName}");
}

var rewardShareFile = "gateway_reward_share.1676167324554.gz";

ListObjectsV2Request v2Request = new ListObjectsV2Request
{
    BucketName = "foundation_iot_packet_ingest",
    StartAfter = "packetreport.1681334821566.gz"
};

//for (s3Client.ListObjectsV2Async(v2Request))
//{

//};

var getObjectResult = await s3Client.GetObjectAsync("foundation-iot-verified-rewards", rewardShareFile);
using var goStream = getObjectResult.ResponseStream;

string tempFile = @"c:\temp\gateway_reward_share.1676167324554.gz";
using MemoryStream memoryStream = new MemoryStream();

goStream.CopyTo(memoryStream);

memoryStream.Position = 0;
using Stream streamToWriteTo = File.Open(tempFile, FileMode.Create);
await memoryStream.CopyToAsync(streamToWriteTo);
memoryStream.Seek(0, SeekOrigin.Begin);

var unzip = DecompressSteam(memoryStream);
if (unzip.Length < 5)
    return;

using MemoryStream unzipStream = new MemoryStream(unzip);
using Stream streamToWriteTo2 = File.Open(@"c:\temp\gateway_reward_share.1676167324554", FileMode.Create);
await unzipStream.CopyToAsync(streamToWriteTo2);

int m_size_a = MessageSize(unzip, 0);
Console.WriteLine($"m_size = {m_size_a}");

//await BucketListAsync(s3Client, "foundation-iot-verified-rewards");

var mLists = ParseRawData(unzip);

List<gateway_reward_share> rewardList = new List<gateway_reward_share>();
foreach (var item in mLists)
{
    var gdata = gateway_reward_share.Parser.ParseFrom(item);
    rewardList.Add(gdata);
    //Console.WriteLine(gdata);
}

var rewardProto = new gateway_reward_share
{
    BeaconAmount = 0,
    EndPeriod = 0
};

var rewardListProto = new gateway_reward_shares();
rewardListProto.RewardShare.Add(rewardList);
var rewardProtoBytes = rewardListProto.ToByteArray();

using MemoryStream byteStream = new MemoryStream(rewardProtoBytes);
using Stream streamToWriteTo3 = File.Open(@"c:\temp\gateway_reward_share.1676167324554.proto", FileMode.Create);
await byteStream.CopyToAsync(streamToWriteTo3);

string parquetFileName2 = @"c:\temp\gateway_reward_share.1676167324554.parquet";
using var rowWriter2 = ParquetFile.CreateRowWriter<ParquetGatewayReward>(parquetFileName2);

List<ParquetGatewayReward> reports2 = new List<ParquetGatewayReward>();

foreach(var reward in rewardList)
{
    var item = new ParquetGatewayReward
    {
        HotspotKey = reward.HotspotKey.ToStringUtf8(),
        BeaconAmount = reward.BeaconAmount,
        WitnessAmount = reward.WitnessAmount,
        StartPeriod = reward.StartPeriod,
        EndPeriod = reward.EndPeriod,
    };
    reports2.Add(item);
}

rowWriter2.WriteRows(reports2);
rowWriter2.StartNewRowGroup();

//string path = @"c:\temp\MyTest.txt";
string path = @"c:\temp\packetreport.1666990455151.gz";
return;

// Delete the file if it exists.  
if (!File.Exists(path))
{
    Console.WriteLine("File Not Found");
}
else
{
    string parquetFileName = "packet_report.parquet";
    Console.WriteLine($"File {path} Found");
    byte[] file = File.ReadAllBytes(path);
    byte[] data = Decompress(file);
    Console.WriteLine(file.Length);
    Console.WriteLine(data.Length);

    IEnumerable<ParquetReport> reports = new List<ParquetReport>();

    if (File.Exists(parquetFileName))
    {
        using var rowReader = ParquetFile.CreateRowReader<ParquetReport>(parquetFileName);
        for (int rowGroup = 1; rowGroup < rowReader.FileMetaData.NumRowGroups; ++rowGroup)
        {
            var group = rowReader.ReadRows(rowGroup);
            reports.Concat(group.ToImmutableList());
        }
        rowReader.Dispose();
    }

    //using var stream = new FileStream("packet_report.parquet", FileMode.OpenOrCreate);
    //using var rwFile = new ManagedRandomAccessFile(stream);
    using var rowWriter = ParquetFile.CreateRowWriter<ParquetReport>(parquetFileName);

    if (reports.LongCount() > 0)
    {
        rowWriter.WriteRows(reports);
        rowWriter.StartNewRowGroup();
    }

    HashSet<uint> freqSet = new HashSet<uint>();
    HashSet<string> gatewaySet = new HashSet<string>();
    HashSet<string> regionSet = new HashSet<string>();
    HashSet<int> dataRateSet = new HashSet<int>();

    do
    {
        if (data.Length < 5)
            break;

        var bytes_4 = data.Take(4).ToArray();
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes_4);
        int m_size = BitConverter.ToInt32(bytes_4, 0);
        Console.WriteLine($"m_size = {m_size}");

        var message = data.Skip(4).ToArray().Take(m_size).ToArray();

        var mData = packet_router_packet_report_v1.Parser.ParseFrom(message);
        Console.WriteLine(mData);

        var row = PopulateParquetRow(message);

        freqSet.Add(row.Frequency);
        gatewaySet.Add(row.Gateway);
        regionSet.Add(row.Region);
        dataRateSet.Add(row.DataRate);

        rowWriter.WriteRow(row);

        var rows2 = rowWriter.FileMetaData?.NumRows;
        Console.WriteLine($"rows = {rows2}");

        data = data.Skip(4 + m_size).ToArray();

    } while (true);

    rowWriter.Close();
}

/// <summary>
/// This method uses a paginator to retrieve the list of objects in an
/// an Amazon S3 bucket.
/// </summary>
/// <param name="client">An Amazon S3 client object.</param>
/// <param name="bucketName">The name of the S3 bucket whose objects
/// you want to list.</param>
static async Task BucketListAsync(IAmazonS3 client, string bucketName)
{
    var listObjectsV2Paginator = client.Paginators.ListObjectsV2(new ListObjectsV2Request
    {
        BucketName = bucketName
    });

    await foreach (var response in listObjectsV2Paginator.Responses)
    {
        Console.WriteLine($"HttpStatusCode: {response.HttpStatusCode}");
        Console.WriteLine($"Number of Keys: {response.KeyCount}");
        foreach (var entry in response.S3Objects)
        {
            Console.WriteLine($"Key = {entry.Key} Size = {entry.Size}");
        }
    }
}

static int MessageSize(byte[] data, int offset)
{
    var size_bytes = new byte[4];
    Array.Copy(data, offset, size_bytes, 0, 4);
    if (BitConverter.IsLittleEndian)
        Array.Reverse(size_bytes);
    int size = BitConverter.ToInt32(size_bytes, 0);
    return size;
}

static List<byte[]> ParseRawData(byte[] data)
{
    List<byte[]> list = new List<byte[]>();
    var size_bytes = new byte[4];
    int offset = 0;
    do
    {
        if (offset > (data.Length - 5))
            break;

        Array.Copy(data, offset, size_bytes, 0, 4);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(size_bytes);
        int m_size = BitConverter.ToInt32(size_bytes, 0);

        //var message = data.Skip(4).ToArray().Take(m_size).ToArray();
        offset += 4;
        var message = new byte[m_size];
        Array.Copy(data, offset, message, 0, m_size);

        list.Add(message);

        //data = data.Skip(4 + m_size).ToArray();
        offset += m_size;

    } while (true);

    return list;
}

static ParquetReport PopulateParquetRow(byte[]? message)
{
    var mData = packet_router_packet_report_v1.Parser.ParseFrom(message);

    var parquetRow = new ParquetReport
    {
        GatewayTimestamp = (long)mData.GatewayTimestampMs,
        OUI = mData.Oui,
        NetID = mData.NetId,
        RSSI = mData.Rssi,
        Frequency = mData.Frequency,
        SNR = mData.Snr,
        DataRate = (int)mData.Datarate,
        Region = mData.Region.ToString(),
        Gateway = mData.Gateway.ToBase64(),
        PayloadHash = mData.Gateway.ToBase64(),
        PayloadSize = mData.PayloadSize
    };

    return parquetRow;
}

static void PrintMessage(IMessage message)
{
    var descriptor = message.Descriptor;
    foreach (var field in descriptor.Fields.InDeclarationOrder())
    {
        Console.WriteLine(
            "Field {0} ({1}): {2}",
            field.FieldNumber,
            field.Name,
            field.Accessor.GetValue(message));
    }
}

static byte[] Decompress(byte[] gzip)
{
    // Create a GZIP stream with decompression mode.
    // ... Then create a buffer and write into while reading from the GZIP stream.
    using (GZipStream stream = new GZipStream(new MemoryStream(gzip), CompressionMode.Decompress))
    {
        const int size = 4096;
        byte[] buffer = new byte[size];
        using (MemoryStream memory = new MemoryStream())
        {
            int count = 0;
            do
            {
                count = stream.Read(buffer, 0, size);
                if (count > 0)
                {
                    memory.Write(buffer, 0, count);
                }
            }
            while (count > 0);
            return memory.ToArray();
        }
    }
}


static byte[] DecompressSteam(Stream gzip)
{
    // Create a GZIP stream with decompression mode.
    // ... Then create a buffer and write into while reading from the GZIP stream.
    using (GZipStream stream = new GZipStream(gzip, CompressionMode.Decompress))
    {
        const int size = 4096;
        byte[] buffer = new byte[size];
        using (MemoryStream memory = new MemoryStream())
        {
            int count = 0;
            do
            {
                count = stream.Read(buffer, 0, size);
                if (count > 0)
                {
                    memory.Write(buffer, 0, count);
                }
            }
            while (count > 0);
            return memory.ToArray();
        }
    }
}

/// <summary>
/// Shows how to list the objects in an Amazon S3 bucket.
/// </summary>
/// <param name="client">An initialized Amazon S3 client object.</param>
/// <param name="bucketName">The name of the bucket for which to list
/// the contents.</param>
/// <returns>A boolean value indicating the success or failure of the
/// copy operation.</returns>
static async IAsyncEnumerable<string> ListBucketContentsAsync(IAmazonS3 client, string bucketName, string startAfter)
{
    //try
    //{
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            StartAfter = startAfter,
            MaxKeys = 5,
        };

        Console.WriteLine("--------------------------------------");
        Console.WriteLine($"Listing the contents of {bucketName}:");
        Console.WriteLine("--------------------------------------");

        ListObjectsV2Response response;

        do
        {
            response = await client.ListObjectsV2Async(request);

            var s3Obj = response.S3Objects;
            foreach( var item in s3Obj)
            {
                var key = item.Key;
                yield return key;
            };
                //.ForEach(obj => yieldAwaitable obj.key);
                //.ForEach(obj => Console.WriteLine($"{obj.Key,-35}{obj.LastModified.ToShortDateString(),10}{obj.Size,10}"));

            // If the response is truncated, set the request ContinuationToken
            // from the NextContinuationToken property of the response.
            request.ContinuationToken = response.NextContinuationToken;
        }
        while (response.IsTruncated);

        //return true;
    //}
    //catch (AmazonS3Exception ex)
    //{
        //Console.WriteLine($"Error encountered on server. Message:'{ex.Message}' getting list of objects.");
        //return false;
    //}
}

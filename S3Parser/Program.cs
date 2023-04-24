// See https://aka.ms/new-console-template for more information
// To interact with Amazon S3.
using Amazon.S3;
using Amazon.S3.Model;
using Google.Protobuf;
using Helium.PacketRouter;
using Parquet.Serialization;
using Parquet.File;
using System.IO.Compression;
using System.Text.RegularExpressions;
using Google.Protobuf.WellKnownTypes;

Console.WriteLine("Hello, World!");

int minutes = 30;
var startString = ToUnixEpochTime("2023-4-22 12:00:00 AM"); // 1680332400;

string parFile = @"C:\temp\packet_report_4-1.parquet";
string parFile2 = @"C:\temp\packetreport_1680073200.parquet";


//using Stream parStream = File.Open(parFile2, FileMode.OpenOrCreate);

//IList<ParquetReport> readData = await ParquetSerializer.DeserializeAsync<ParquetReport>(parStream);
//var rdCount = readData.Count;

//var options = new ParquetOptions { TreatByteArrayAsString = true };
//using (var reader = await ParquetReader.CreateAsync(parStream, options))
//{

//    var fields = reader.Schema.GetDataFields();
//    foreach (var field in fields)
//    {
//        Console.WriteLine($"field={field}");
//    }
//    var groupCount = reader.RowGroupCount;
//    Console.WriteLine($"groupCount={groupCount}");


//    var groupReader = reader.OpenRowGroupReader(0);

//    DataColumn[] data = await reader.ReadEntireRowGroupAsync(1);


//    var len = data[0].Data.Length;
//    Console.WriteLine($"len={len}");
//    var x1 = data[0].Data;

//    Int64[] firstCol = (Int64[])x1;

//    //var column = await groupReader.ReadColumnAsync(fields[0]);

//    Console.WriteLine($"fields={fields}");
//}



//Table tbl = await Table.ReadAsync(parFile);
//IEnumerable<PacketReport> tblList = tbl.ToList<PacketReport>();

//IList<PacketReport> data = await ParquetSerializer.DeserializeAsync<PacketReport>(parStream);

#if false
using var fileReader = new ParquetFileReader(parFile);

int numColumns = fileReader.FileMetaData.NumColumns;
long numRows = fileReader.FileMetaData.NumRows;
int numRowGroups = fileReader.FileMetaData.NumRowGroups;
IReadOnlyDictionary<string, string> metadata = fileReader.FileMetaData.KeyValueMetadata;

SchemaDescriptor schema = fileReader.FileMetaData.Schema;
for (int columnIndex = 0; columnIndex < schema.NumColumns; ++columnIndex)
{
    ColumnDescriptor column = schema.Column(columnIndex);
    string columnName = column.Name;
    var columnType = column.LogicalType;
    Console.WriteLine($"columnName={columnName} type={columnType}");
}

for (int rowGroup = 1; rowGroup < fileReader.FileMetaData.NumRowGroups; ++rowGroup)
{
    using var rowGroupReader = fileReader.RowGroup(rowGroup);
    var groupNumRows = checked((int)rowGroupReader.MetaData.NumRows);

    var groupTimestamps = rowGroupReader.Column(0).LogicalReader<Int64>().ReadAll(100);
    var groupOUIs = rowGroupReader.Column(1).LogicalReader<UInt64>().ReadAll(100);
    var groupNetIDs = rowGroupReader.Column(2).LogicalReader<UInt64>().ReadAll(100);
}

fileReader.Close();
#endif
//if (File.Exists(parFile))
//{
//    using var rowReader = ParquetFile.CreateRowReader<ParquetReport>(parFile);
//    for (int rowGroup = 0; rowGroup < rowReader.FileMetaData.NumRowGroups; ++rowGroup)
//    {
//        using var rowGroupReader = rowReader.R RowGroup(rowGroup);
//        ParquetReport[] rowGroupReader = rowReader.ReadRows(rowGroup);
//        var groupList = rowGroupReader.ToImmutableList();
//        //reports.Concat(group.ToImmutableList());
//        foreach (var item in groupList)
//        {
//            Console.WriteLine($"item={item}");
//        }
//    }
//    rowReader.Dispose();
//}

// Create an S3 client object.
var s3Client = new AmazonS3Client();

var bucketList = ListBucketsAsync(s3Client);
await foreach( var name in bucketList )
{
    Console.WriteLine($"Bucket {name}");
}

string ingestBucket = "foundation-iot-packet-ingest";
if (!bucketList.ToBlockingEnumerable<string>().ToList().Contains( ingestBucket ))
{
    throw new Exception($"{ingestBucket} not found");
}

ulong startUnixExpected = 1680332653569;
ulong startUnix = ulong.Parse(startString);

string startAfterExpected = $"packetreport.{startUnixExpected}";
string startAfter = $"packetreport.{startString}";

HashSet<ByteString> hashSet = new HashSet<ByteString>();
var prevTimestamp = DateTime.MinValue;

int currCount = 0;
int currDupeCount = 0;
UInt64 byteCount = 0;
UInt64 rawCount = 0;
long gzCount = 0;
int fileCount = 0;
ulong unit24Count = 0;

string parquetFileName2 = $"c:\\temp\\packetreport_{startString}.parquet";

Dictionary<ulong, ulong> ouiCounter = new Dictionary<ulong, ulong>();
Dictionary<ulong, ulong> regionCounter = new Dictionary<ulong, ulong>();
List<ParquetReport> packetList = new List<ParquetReport>();

//var list = ListBucketContentsAsync(s3Client, ingestBucket, startAfter);
var list = ListBucketKeysAsync(s3Client, ingestBucket, startUnix, minutes);
await foreach( var item in list)
{
    //Console.WriteLine($"report: {item}");
    var meta  = await S3ObjectMeta(s3Client, ingestBucket, item);
    var timestamp = meta.Item1;
    if (TimeBoundaryTrigger(prevTimestamp, timestamp))
    {
        Console.WriteLine($"Time trigger {prevTimestamp} {timestamp}");
        Console.WriteLine($"{item} count={currCount} dupes={currDupeCount} bytes={byteCount}");
        var fees2 = ((double)byteCount / 24) * 0.00001;
        Console.WriteLine($"{startUnix} minutes={minutes} countTotal={currCount} byteTotal={byteCount} u24Count={unit24Count} fc={fileCount} rawTotal={(float)rawCount / (1024 * 1024)} gzTotal={(float)gzCount / (1024 * 1024)} fees={fees2}");

    }

    if (hashSet.Count > 5000)
    {
        hashSet.Clear();
    }

    var reportStats = await GetPacketReportsAsync(s3Client, hashSet, ouiCounter, regionCounter, packetList, ingestBucket, item);
    var timestamp2 = Regex.Match(item, @"\d+").Value;
    UInt64 microSeconds = UInt64.Parse(timestamp2);
    var time = UnixTimeStampToDateTime(microSeconds);
    Console.WriteLine($"{item} {reportStats} {time.ToShortTimeString()}");

    currCount += reportStats.Item1;
    currDupeCount += reportStats.Item2;
    byteCount += reportStats.Item3;
    unit24Count += reportStats.Item4;
    rawCount += reportStats.Item5;
    gzCount += meta.Item2;
    fileCount++;

    prevTimestamp = timestamp;
}

var predictedDC = (double)unit24Count * 0.00001;
var fees = ((double)byteCount / 24) * 0.00001;
Console.WriteLine($"{startUnix} minutes={minutes} countTotal= {currCount} dupeTotal= {currDupeCount} byteTotal= {byteCount} predDC= {predictedDC} fc= {fileCount} rawTotal= {(float)rawCount / (1024 * 1024)} gzTotal= {(float)gzCount / (1024 * 1024)} fees= {fees}");

var vpList = ComputeValuePercent(byteCount, ouiCounter);
foreach(var vp in vpList)
{
    Console.WriteLine($"OUI= {vp.Item1} Percentage= {vp.Item2} %");
};

var vpList2 = ComputeValuePercent(byteCount, regionCounter);
foreach (var vp in vpList2)
{
    Console.WriteLine($"Region= {vp.Item1} Percentage= {vp.Item2} %");
};

await ParquetSerializer.SerializeAsync(packetList, parquetFileName2);

static List<(ulong, double)> ComputeValuePercent(double byteCount, Dictionary<ulong, ulong> valueCounter)
{
    List<(ulong, double)> valuePercentList = new List<(ulong, double)>();
    long k = 10;
    ulong[] result = new ulong[k];

    PriorityQueue<ulong, ulong> heap = new PriorityQueue<ulong, ulong>(
        Comparer<ulong>.Create((x, y) => (int)(x - y)));

    heap.Clear();
    foreach (var freqEntry in valueCounter)
    {
        heap.Enqueue(freqEntry.Key, freqEntry.Value);
        if (heap.Count > k)
            heap.Dequeue();
    }

    long k2 = Math.Min(k, heap.Count);
    for (long i = k2 - 1; i >= 0; i--)
    {
        if (heap.Count == 0)
            break;
        result[i] = heap.Dequeue();
    }

    for (int i = 0; i < k2; i++)
    {
        ulong value = result[i];
        if (valueCounter.TryGetValue(value, out ulong count))
        {
            var regionPercentage = (double)count * 100 / (double)byteCount;
            valuePercentList.Add((value, regionPercentage));
        }
    }
    return valuePercentList;
}

static async void WriteBytesToFile(byte[] bytes, string fileName)
{
    using MemoryStream byteStream = new MemoryStream(bytes);
    using Stream streamToWriteTo = File.Open(fileName, FileMode.Create);
    await byteStream.CopyToAsync(streamToWriteTo);
}

static Stream ReadStreamToFile(string filename)
{
    return File.Open(filename, FileMode.Open);
}

static async void WriteStreamToFile(Stream inStream, string filename)
{
    using MemoryStream memoryStream = new MemoryStream();
    inStream.CopyTo(memoryStream);
    memoryStream.Position = 0;
    using Stream streamToWriteTo = File.Open(filename, FileMode.Create);
    await memoryStream.CopyToAsync(streamToWriteTo);
    memoryStream.Seek(0, SeekOrigin.Begin);
}

static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
{
    // Unix timestamp is seconds past epoch
    DateTime dateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
    dateTime = dateTime.AddMilliseconds(unixTimeStamp).ToLocalTime();
    return dateTime;
}

static string ToUnixEpochTime(string textDateTime)
{
    // textString "2023-4-12 2:27:01 PM"
    var dateTime = DateTime.Parse(textDateTime);

    DateTimeOffset dto = new DateTimeOffset(dateTime.ToUniversalTime());
    return dto.ToUnixTimeSeconds().ToString();
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
    if (data.Length < 4)
        return 0;
    var size_bytes = new byte[4];
    Array.Copy(data, offset, size_bytes, 0, 4);
    if (BitConverter.IsLittleEndian)
        Array.Reverse(size_bytes);
    int size = BitConverter.ToInt32(size_bytes, 0);
    return size;
}

static List<byte[]> ExtractMessageList(byte[] data)
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
        GatewayTimestamp = mData.GatewayTimestampMs,
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

static async Task<(int, int, UInt64, ulong, UInt64)> GetPacketReportsAsync(
    AmazonS3Client s3Client,
    HashSet<ByteString> hashSet,
    Dictionary<ulong, ulong> ouiCounter,
    Dictionary<ulong, ulong> regionCounter,
    List<ParquetReport> packetList,
    string bucketName,
    string report)
{
    var rawBytes = await DecompressS3Object(s3Client, bucketName, report);
    int messageCount = 0;
    int dupeCount = 0;
    ulong totalBytes = 0;
    ulong predictedDC = 0;

    var messageList = ExtractMessageList(rawBytes);
    foreach (var message in messageList)
    {
        var mData = packet_router_packet_report_v1.Parser.ParseFrom(message);
        var hash = mData.PayloadHash;
        if (hashSet.Contains(hash))
        {
            dupeCount++;
            continue;
        }

        var pData = new ParquetReport()
        {
            GatewayTimestamp = mData.GatewayTimestampMs,
            OUI = mData.Oui,
            NetID = mData.NetId,
            RSSI = mData.Rssi,
            Frequency = mData.Frequency,
            SNR = mData.Snr,
            Region = mData.Region.ToString(),
            Gateway = mData.Gateway.ToBase64(),
            PayloadHash = mData.PayloadHash.ToBase64(),
            PayloadSize = mData.PayloadSize
        };
        packetList.Add(pData);

        hashSet.Add(hash);
        messageCount++;
        totalBytes += mData.PayloadSize;
        ulong dc = (mData.PayloadSize / 24) + 1;
        predictedDC += dc;
        ulong oui = mData.Oui;
        ulong region = (ulong)mData.Region;

        if (!(ouiCounter.ContainsKey(oui)))
        {
            ouiCounter.Add(oui, mData.PayloadSize);
        }
        else
        {
            ouiCounter[oui] += mData.PayloadSize;
        }

        if (!(regionCounter.ContainsKey(region)))
        {
            regionCounter.Add(region, mData.PayloadSize);
        }
        else
        {
            regionCounter[region] += mData.PayloadSize;
        }
    }

    return (messageCount, dupeCount, totalBytes, predictedDC, (UInt64)rawBytes.Length);
}

static bool TimeBoundaryTrigger(DateTime prior, DateTime later)
{
    if (prior.Minute != later.Minute)
    {
        for (int i = prior.Minute + 1; i <= later.Minute; i++)
        {
            if (i % 20 == 0)
            {
                return true;
            }
        }
    }
    return false;
}

static async Task<(DateTime, long)> S3ObjectMeta(AmazonS3Client s3Client, string bucketName, string keyName)
{
    var getObjectResult = await s3Client.GetObjectAsync(bucketName, keyName);
    return (getObjectResult.LastModified, getObjectResult.Headers.ContentLength);
}

static async Task<byte[]> DecompressS3Object(AmazonS3Client s3Client, string bucketName, string keyName)
{
    var getObjectResult = await s3Client.GetObjectAsync(bucketName, keyName);
    using var goStream = getObjectResult.ResponseStream;

    using MemoryStream memoryStream = new MemoryStream();

    goStream.CopyTo(memoryStream);

    memoryStream.Position = 0;
    memoryStream.Seek(0, SeekOrigin.Begin);

    var unzip = DecompressSteam(memoryStream);
    return unzip;
}

static byte[] DecompressGzipBytes(byte[] gzip)
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
            var byteArray = memory.ToArray();
            //Console.WriteLine($"gzip={gzip.Length / 1024} raw={byteArray.Length / 1024}");
            return memory.ToArray();
        }
    }
}

static async IAsyncEnumerable<string> ListBucketsAsync(IAmazonS3 s3Client)
{
    var listResponse = await s3Client.ListBucketsAsync();
    foreach (var b in listResponse.Buckets)
    {
        yield return b.BucketName;
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

static async IAsyncEnumerable<string> ListBucketKeysAsync(IAmazonS3 client, string bucketName, UInt64 unixTime, int minutes)
{
    if (unixTime < 10_000_000_000)
        unixTime = unixTime * 1000;

    string startAfter = $"packetreport.{unixTime}";

    var request = new ListObjectsV2Request
    {
        BucketName = bucketName,
        StartAfter = startAfter,
        MaxKeys = 100,
    };

    Console.WriteLine("--------------------------------------");
    Console.WriteLine($"Iterating the keys of {bucketName}:");
    Console.WriteLine("--------------------------------------");

    ListObjectsV2Response response;

    do
    {
        response = await client.ListObjectsV2Async(request);

        var s3Obj = response.S3Objects;
        foreach (var item in s3Obj)
        {
            var timestamp = Regex.Match(item.Key, @"\d+").Value;
            UInt64 microSeconds = UInt64.Parse(timestamp);
            var timeDiff = microSeconds - unixTime;
            if (timeDiff > (ulong)minutes * 60 * 1000)
            {
                s3Obj.Clear();
                response.IsTruncated = false;
                break;
            }
            var key = item.Key;
            yield return key;
        };
        request.ContinuationToken = response.NextContinuationToken;
    }
    while (response.IsTruncated);
}

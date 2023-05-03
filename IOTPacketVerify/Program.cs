// See https://aka.ms/new-console-template for more information
// To interact with Amazon S3.
using Amazon.S3;
using Amazon.S3.Model;
using Google.Protobuf;
using Parquet.Serialization;
using Parquet.File;
using System.IO.Compression;
using System.Text.RegularExpressions;
using Google.Protobuf.WellKnownTypes;
using System.CommandLine;
using Helium.PacketVerifier;

int minutes = 24 * 60;
var startString = ToUnixEpochTime("2023-4-23Z"); // "2023-4-23 12:00:00 AM" or 1680332400;

if (args.Length > 0)
{
    var startOption = new Option<string?>(
        name: "--starttime",
        description: "The time to start processing files.",
        getDefaultValue: () => "2023-4-1"
    );


    var durationOption = new Option<int?>(
        name: "--duration",
        description: "The number of minutes to process, e.g. duration is 60 minutes",
        getDefaultValue: () => 10
    );

    var rootCommand = new RootCommand("Sample app for System.CommandLine");
    rootCommand.AddOption(startOption);
    rootCommand.AddOption(durationOption);

    rootCommand.SetHandler((inStartTime, inMinutes) =>
    {
        startString = ToUnixEpochTime(inStartTime);
        minutes = inMinutes.GetValueOrDefault(10);
    }, startOption, durationOption);

    await rootCommand.InvokeAsync(args);
}
else
{
    Console.WriteLine("No arguments");
}

ulong startUnixExpected = 1680332653569;
ulong startUnix = ulong.Parse(startString);

string startAfterExpected = $"iot_valid_packet.{startUnixExpected}";
string startAfter = $"iot_valid_packet.{startString}";

var dateTime = UnixTimeMillisecondsToDateTime(double.Parse(startString) * 1000);

Console.WriteLine($"Start time is {dateTime.ToUniversalTime()}");
Console.WriteLine($"S3 startAfter file is {startAfter}");
Console.WriteLine($"Duration is {minutes} minutes");

string parFile = @"C:\temp\packet_report_4-1.parquet";
string parFile2 = @"C:\temp\iot_valid_packet_1680073200.parquet";

List<ParquetReport> packetList = new List<ParquetReport>();
//packetList = null;

// Create an S3 client object.
var s3Client = new AmazonS3Client();

string ingestBucket = "foundation-iot-packet-verifier";
var bucketList = ListBucketsAsync(s3Client);
if (!bucketList.ToBlockingEnumerable<string>().ToList().Contains( ingestBucket ))
{
    throw new Exception($"{ingestBucket} not found");
}

HashSet<ByteString> hashSet = new HashSet<ByteString>();
var prevTimestamp = DateTime.MinValue;

int currCount = 0;
int currDupeCount = 0;
UInt64 byteCount = 0;
UInt64 rawCount = 0;
long gzCount = 0;
int fileCount = 0;
ulong unit24Count = 0;

string parquetFileName2 = $"c:\\temp\\iot_valid_packet_{startString}.parquet";

//var list = ListBucketContentsAsync(s3Client, ingestBucket, startAfter);
var list = ListBucketKeysAsync(s3Client, ingestBucket, startUnix, minutes);
await foreach( var item in list)
{
    //Console.WriteLine($"report: {item}");
    var meta  = await S3ObjectMeta(s3Client, ingestBucket, item);
    var timestamp = meta.Item1;
    if (TimeBoundaryTrigger(prevTimestamp, timestamp))
    {
        Console.WriteLine($"Delta {prevTimestamp.ToUniversalTime().ToShortTimeString()} {timestamp.ToUniversalTime().ToShortTimeString()}");
        var fees2 = (unit24Count) * 0.00001;
        Console.WriteLine($"Cumulative countTotal={currCount} dupes={currDupeCount} byteTotal={byteCount} u24Count={unit24Count} fc={fileCount} rawTotal={(float)rawCount / (1024 * 1024)} gzTotal={(float)gzCount / (1024 * 1024)} fees={fees2.ToString("######.#")}");

    }

    if (hashSet.Count > 5000)
    {
        hashSet.Clear();
    }

    var reportStats = await GetValidPacketsAsync(s3Client, hashSet, packetList, ingestBucket, item);
    var timestamp2 = Regex.Match(item, @"\d+").Value;
    UInt64 microSeconds = UInt64.Parse(timestamp2);
    var time = UnixTimeMillisecondsToDateTime(microSeconds);
    Console.WriteLine($"{time.ToUniversalTime().ToShortTimeString()} {item} {reportStats}");

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
var fees = (predictedDC) * 0.00001;

Console.WriteLine("--------------------------------------");
Console.WriteLine($"Start time is {dateTime.ToUniversalTime()}");
Console.WriteLine($"S3 startAfter file is {startAfter}");
Console.WriteLine($"Duration is {minutes} minutes");
Console.WriteLine("--------------------------------------");
Console.WriteLine($"{startUnix} minutes={minutes} loraMsgTotal= {currCount} dupeTotal= {currDupeCount} byteTotal= {byteCount} dcCount= {unit24Count} fc= {fileCount} rawTotal= {(float)rawCount / (1024 * 1024)} gzTotal= {(float)gzCount / (1024 * 1024)} fees= {fees}");
Console.WriteLine("--------------------------------------");

if (packetList is not null)
{
    await ParquetSerializer.SerializeAsync(packetList, parquetFileName2);
}

static async Task<(int, int, UInt64, ulong, UInt64)> GetValidPacketsAsync(
    AmazonS3Client s3Client,
    HashSet<ByteString> hashSet,
    List<ParquetReport>? packetList,
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
        var mData = valid_packet.Parser.ParseFrom(message);
        var hash = mData.PayloadHash;
        if (hashSet.Contains(hash))
        {
            dupeCount++;
            //continue;
        }

        if (packetList is not null)
        {
            var pData = new ParquetReport()
            {
                PacketTimestamp = mData.PacketTimestamp,
                Gateway = mData.Gateway.ToBase64(),
                PayloadHash = mData.PayloadHash.ToBase64(),
                PayloadSize = mData.PayloadSize,
                NumDCs = mData.NumDcs
            };
            packetList.Add(pData);
        }

        hashSet.Add(hash);
        messageCount++;
        totalBytes += mData.PayloadSize;
        predictedDC += mData.NumDcs;
    }

    return (messageCount, dupeCount, totalBytes, predictedDC, (UInt64)rawBytes.Length);
}

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

static DateTime UnixTimeMillisecondsToDateTime(double unixTimeStamp)
{
    // Unix timestamp is seconds past the 1970 epoch
    DateTime dateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
    dateTime = dateTime.AddMilliseconds(unixTimeStamp).ToLocalTime();
    return dateTime;
}

static string ToUnixEpochTime(string textDateTime)
{
    // textString "2023-4-12 2:27:01 PM"
    var dateTime = DateTime.Parse( textDateTime).ToUniversalTime();

    DateTimeOffset dto = new DateTimeOffset(dateTime);
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
    var mData = valid_packet.Parser.ParseFrom(message);

    var parquetRow = new ParquetReport
    {
        PacketTimestamp = mData.PacketTimestamp,
        Gateway = mData.Gateway.ToBase64(),
        PayloadHash = mData.Gateway.ToBase64(),
        PayloadSize = mData.PayloadSize,
        NumDCs = mData.NumDcs
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
    return (getObjectResult.LastModified.ToUniversalTime(), getObjectResult.Headers.ContentLength);
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

    string startAfter = $"iot_valid_packet.{unixTime}";

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

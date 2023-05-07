// See https://aka.ms/new-console-template for more information
using Amazon.S3;
using Amazon.S3.Model;
using Google.Protobuf;
using Helium.PacketVerifier;
using System.CommandLine;
using System.Diagnostics;
using System.IO.Compression;
using System.Text.RegularExpressions;

int minutes = 24 * 60;
var startString = ToUnixEpochTime("2023-4-19Z"); // "2023-4-23 12:00:00 AM" or 1680332400;
string ingestBucket = "foundation-iot-packet-verifier";
string keyPrefix = "iot_valid_packet";

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

var stopWatch = Stopwatch.StartNew();
ulong startUnixExpected = 1680332653569;
ulong startUnix = ulong.Parse(startString);

string startAfterExpected = $"{keyPrefix}.{startUnixExpected}";
string startAfter = $"{keyPrefix}.{startString}";

var dateTime = UnixTimeMillisecondsToDateTime(double.Parse(startString) * 1000);

Console.WriteLine("--------------------------------------");
Console.WriteLine($"Start time is {dateTime.ToUniversalTime()}");
Console.WriteLine($"S3 startAfter file is {startAfter}");
Console.WriteLine($"Duration is {minutes} minutes");
Console.WriteLine("--------------------------------------");

List<ParquetReport> packetList = new List<ParquetReport>();
packetList = null;

// Create an S3 client object.
var s3Client = new AmazonS3Client();

var bucketList = ListBucketsAsync(s3Client);
if (!bucketList.ToBlockingEnumerable<string>().ToList().Contains(ingestBucket))
{
    throw new Exception($"{ingestBucket} not found");
}

HashSet<ByteString> hashSet = new HashSet<ByteString>();
var prevTimestamp = DateTime.MinValue;

PacketSummary theSummary = new PacketSummary();
List<string> itemList = new List<string>();

var list = ListBucketKeysAsync(s3Client, ingestBucket, startUnix, minutes).ToBlockingEnumerable();
var last = list.Last();
var listCount = list.Count();
Console.WriteLine($"Starting {ingestBucket} of key size {listCount}");
foreach (var item in list)
{
    itemList.Add(item);
    if (itemList.Count >= 16 || item == last)
    {
        var taskList = LoopFiles(s3Client, ingestBucket, itemList);
        while (taskList.Any())
        {
            Task<PacketSummary> finishedTask = await Task<PacketSummary>.WhenAny(taskList);
            taskList.Remove(finishedTask);
            var taskSummary = await finishedTask;
            theSummary.MessageCount += taskSummary.MessageCount;
            theSummary.DupeCount += taskSummary.DupeCount;
            theSummary.DCCount += taskSummary.DCCount;
            theSummary.TotalBytes += taskSummary.TotalBytes;
            theSummary.FileCount += taskSummary.FileCount;
            theSummary.RawSize += taskSummary.RawSize;
            theSummary.GzipSize += taskSummary.GzipSize;

            var timestamp2 = Regex.Match(item, @"\d+").Value;
            UInt64 microSeconds = UInt64.Parse(timestamp2);
            var time = UnixTimeMillisecondsToDateTime(microSeconds);
            Console.WriteLine($"{time.ToUniversalTime().ToShortTimeString()} {item} {taskSummary.ToString()}");
        }
        itemList.Clear();
    }
}

var burnedDCFees = (theSummary.DCCount) * 0.00001;
stopWatch.Stop();

Console.WriteLine("--------------------------------------");
Console.WriteLine($"Start time is {dateTime.ToUniversalTime()}");
Console.WriteLine($"S3 startAfter file is {startAfter}");
Console.WriteLine($"Duration is {minutes} minutes");
Console.WriteLine($"Elapsed time is {stopWatch.Elapsed}");
Console.WriteLine("--------------------------------------");
Console.WriteLine($"{startUnix} minutes={minutes} loraMsgTotal= {theSummary.MessageCount} dupes= {theSummary.DupeCount} byteTotal= {theSummary.TotalBytes} dcCount= {theSummary.DCCount} fc= {theSummary.FileCount} rawTotal= {(float)theSummary.RawSize / (1024 * 1024)} gzTotal= {(float)theSummary.GzipSize / (1024 * 1024)} fees= {burnedDCFees.ToString("########.##")}");
Console.WriteLine("--------------------------------------");

static List<Task<PacketSummary>> LoopFiles(AmazonS3Client s3Client, string ingestBucket, List<string> files)
{
    List<Task<PacketSummary>> taskList = new List<Task<PacketSummary>>();
    foreach (var file in files)
    {
        var summary = GetValidPacketsAsync(s3Client, ingestBucket, file);
        taskList.Add(summary);
    }
    return taskList;
}
static async Task<PacketSummary> GetValidPacketsAsync(
    AmazonS3Client s3Client,
    string bucketName,
    string report)
{
    (var modTime, var gzipSize, var rawSize, var rawBytes) = await DownloadS3Object(s3Client, bucketName, report);
    ulong messageCount = 0;
    ulong dupeCount = 0;
    ulong totalBytes = 0;
    ulong dcCount = 0;

    var messageList = ExtractMessageList(rawBytes);
    foreach (var message in messageList)
    {
        var mData = valid_packet.Parser.ParseFrom(message);
        var hash = mData.PayloadHash;

        //hashSet.Add(hash);
        messageCount++;
        totalBytes += mData.PayloadSize;
        dcCount += mData.NumDcs;
    }

    var summary = new PacketSummary()
    {
        ModTime = modTime,
        MessageCount = messageCount,
        DupeCount = dupeCount,
        TotalBytes = totalBytes,
        DCCount = dcCount,
        FileCount = 1,
        RawSize = rawSize,
        GzipSize = gzipSize
    };

    return summary;
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
    var dateTime = DateTime.Parse(textDateTime).ToUniversalTime();

    DateTimeOffset dto = new DateTimeOffset(dateTime);
    return dto.ToUnixTimeSeconds().ToString();
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

static async Task<(DateTime, ulong, ulong, byte[])> DownloadS3Object(AmazonS3Client s3Client, string bucketName, string keyName)
{
    var getObjectResult = await s3Client.GetObjectAsync(bucketName, keyName);
    var modTime = getObjectResult.LastModified.ToUniversalTime();
    var gzipSize = getObjectResult.Headers.ContentLength;
    using var goStream = getObjectResult.ResponseStream;

    using MemoryStream memoryStream = new MemoryStream();

    goStream.CopyTo(memoryStream);

    memoryStream.Position = 0;
    memoryStream.Seek(0, SeekOrigin.Begin);

    var unzip = DecompressSteam(memoryStream);
    return (modTime, (ulong)gzipSize, (ulong)unzip.LongLength, unzip);
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

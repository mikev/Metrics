﻿using Amazon.S3;
using Amazon.S3.Model;
using Google.Protobuf;
using Helium.PacketRouter;
using System.Collections.Concurrent;
using System.CommandLine;
using System.Diagnostics;
using System.IO.Compression;
using System.Text.Json;
using System.Text.RegularExpressions;

uint minutes = 2; // 24 * 60;
var startString = ToUnixEpochTime("2023-5-8Z"); // "2023-4-27 12:00:00 AM"// 1680332400;
string ingestBucket = "foundation-iot-packet-ingest";
var metricsFile = @"c:\temp\lorawan_metrics.json";
int hashSizeMaximum = 25000;

if (args.Length > 0)
{
    var startOption = new Option<string?>(
        name: "--starttime",
        description: "The time to start processing files.",
        getDefaultValue: () => "2023-4-1"
    );

    var durationOption = new Option<uint?>(
        name: "--duration",
        description: "The number of minutes to process, e.g. duration is 60 minutes",
        getDefaultValue: () => 10
    );

    var outputOption = new Option<string?>(
        name: "--output",
        description: "Output json file. For example",
        getDefaultValue: () => "metrics.json"
    );

    var rootCommand = new RootCommand("Sample app for System.CommandLine");
    rootCommand.AddOption(startOption);
    rootCommand.AddOption(durationOption);
    rootCommand.AddOption(outputOption);

    rootCommand.SetHandler((inStartTime, inMinutes, output) =>
    {
        startString = ToUnixEpochTime(inStartTime);
        minutes = inMinutes.GetValueOrDefault(10);
        metricsFile = output;
    }, startOption, durationOption, outputOption);

    await rootCommand.InvokeAsync(args);
}
else
{
    Console.WriteLine("No arguments");
}

var stopWatch = Stopwatch.StartNew();
ulong startUnixExpected = 1680332653569;
ulong startUnix = ulong.Parse(startString);

string startAfterExpected = $"packetreport.{startUnixExpected}";
string startAfter = $"packetreport.{startString}";

var dateTime = UnixTimeMillisecondsToDateTime(double.Parse(startString) * 1000);

Console.WriteLine($"Start time is {dateTime.ToUniversalTime()}");
Console.WriteLine($"S3 startAfter file is {startAfter}");
Console.WriteLine($"Duration is {minutes} minutes");

// Create an S3 client object.
var s3Client = new AmazonS3Client();

var bucketList = ListBucketsAsync(s3Client);
if (!bucketList.ToBlockingEnumerable<string>().ToList().Contains(ingestBucket))
{
    throw new Exception($"{ingestBucket} not found");
}

ConcurrentBag<int> hashSet = new ConcurrentBag<int>();
var prevTimestamp = DateTime.MinValue;

ConcurrentDictionary<ulong, ulong> ouiCounter = new ConcurrentDictionary<ulong, ulong>();
ConcurrentDictionary<ulong, ulong> regionCounter = new ConcurrentDictionary<ulong, ulong>();
object reportLock = new();

ReportSummary theSummary = new ReportSummary();
List<string> itemList = new List<string>();

var list = ListBucketKeysAsync(s3Client, ingestBucket, startUnix, minutes).ToBlockingEnumerable();
var sortedList = list.OrderBy(s => s).ToList();
var last = sortedList.Last();
var listCount = sortedList.Count();
Console.WriteLine($"Starting {ingestBucket} of key size {listCount}");
foreach (var item in sortedList)
{
    Console.WriteLine($"Key {item}");
}
foreach (var item in sortedList)
{
    itemList.Add(item);
    if (itemList.Count >= 8 || item == last)
    {
        var taskList = LoopFiles(reportLock, s3Client, hashSet, ouiCounter, regionCounter, ingestBucket, itemList);
        while (taskList.Any())
        {
            Task<ReportSummary> finishedTask = await Task<ReportSummary>.WhenAny(taskList);
            taskList.Remove(finishedTask);
            var taskSummary = await finishedTask;
            lock (reportLock)
            {
                theSummary.MessageCount += taskSummary.MessageCount;
                theSummary.DupeCount += taskSummary.DupeCount;
                theSummary.DCCount += taskSummary.DCCount;
                theSummary.TotalBytes += taskSummary.TotalBytes;
                theSummary.FileCount += taskSummary.FileCount;
                theSummary.RawSize += taskSummary.RawSize;
                theSummary.GzipSize += taskSummary.GzipSize;
            }
        }
        itemList.Clear();
        if (hashSet.Count > hashSizeMaximum)
        {
            hashSet.Clear();
        }
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

var metrics = InitMetricsFile(metricsFile, dateTime);

List<OUISummary>? ouiList = metrics?.OUIByDay;
var vpList = ComputeValuePercent(theSummary.DCCount, ouiCounter);
foreach (var vp in vpList)
{
    var ouiItem = new OUISummary()
    {
        Time = dateTime.ToUniversalTime(),
        OUI = (uint)vp.Item1,
        DCCount = vp.Item2,
        Percent = vp.Item3
    };
    ouiList?.Add(ouiItem);
    Console.WriteLine($"OUI= {vp.Item1} DC={vp.Item2} Percentage= {vp.Item3} %");
};

List<RegionSummary>? regionList = metrics?.RegionByDay;
var vpList2 = ComputeValuePercent(theSummary.DCCount, regionCounter);
foreach (var vp in vpList2)
{
    var regionItem = new RegionSummary()
    {
        Time = dateTime.ToUniversalTime(),
        Region = (uint)vp.Item1,
        DCCount = vp.Item2,
        Percent = vp.Item3
    };
    regionList?.Add(regionItem);
    Console.WriteLine($"Region= {vp.Item1} DC={vp.Item2} Percentage= {vp.Item3} %");
};

WriteToMetricsFile(metricsFile, metrics, theSummary, dateTime, minutes);
return;

static LoRaWANMetrics? InitMetricsFile(string metricsFile, DateTime dateTime)
{
    if (!File.Exists(metricsFile))
    {
        LoRaWANMetrics metrics = new LoRaWANMetrics()
        {
            IngestByDay = new List<PacketSummary>(),
            VerifyByDay = new List<PacketSummary>(),
            OUIByDay = new List<OUISummary>(),
            RegionByDay = new List<RegionSummary>()
        };

        string jsonMetrics = JsonSerializer.Serialize(metrics);
        File.WriteAllText(metricsFile, jsonMetrics);
        return metrics;
    }
    else
    {
        var jsonMetrics = File.ReadAllText(metricsFile);

        LoRaWANMetrics? metrics = JsonSerializer.Deserialize<LoRaWANMetrics>(jsonMetrics);

        var items = metrics?.IngestByDay;
        var toRemove = items?.Where(item => item.Time == dateTime.ToUniversalTime()).ToList();
        foreach (var s in toRemove)
        {
            items?.Remove(s);
        }

        var items2 = metrics?.OUIByDay;
        var toRemove2 = items2?.Where(item => item.Time == dateTime.ToUniversalTime()).ToList();
        foreach (var s in toRemove2)
        {
            items2?.Remove(s);
        }

        var items3 = metrics?.RegionByDay;
        var toRemove3 = items3?.Where(item => item.Time == dateTime.ToUniversalTime()).ToList();
        foreach (var s in toRemove3)
        {
            items3?.Remove(s);
        }

        return metrics;
    }
}
static void WriteToMetricsFile(string metricsFile, LoRaWANMetrics? metrics, ReportSummary report, DateTime dateTime, uint duration)
{
    var packetSummary = new PacketSummary()
    {
        Time = dateTime.ToUniversalTime(),
        Duration = duration,
        DCCount = report.DCCount,
        PacketCount = report.MessageCount,
        DupeCount = report.DupeCount,
        PacketBytes = report.TotalBytes,
        Files = report.FileCount,
        RawBytes = report.RawSize,
        GzipBytes = report.GzipSize
    };

    metrics.IngestByDay?.Add(packetSummary);

    JsonSerializerOptions options = new() { WriteIndented = true };
    var jsonMetrics = JsonSerializer.Serialize<LoRaWANMetrics>(metrics, options);

    File.WriteAllText(metricsFile, jsonMetrics);
}

static List<Task<ReportSummary>> LoopFiles(
    object reportLock,
    AmazonS3Client s3Client,
    ConcurrentBag<int> hashSet,
    ConcurrentDictionary<ulong, ulong> ouiCounter,
    ConcurrentDictionary<ulong, ulong> regionCounter,
    string ingestBucket,
    List<string> files)
{
    List<Task<ReportSummary>> taskList = new List<Task<ReportSummary>>();
    foreach (var file in files)
    {
        var summary = GetPacketReportsAsync(reportLock, s3Client, hashSet, ouiCounter, regionCounter, ingestBucket, file);
        taskList.Add(summary);
    }
    return taskList;
}

static async Task<ReportSummary> GetPacketReportsAsync(
    object reportLock,
    AmazonS3Client s3Client,
    ConcurrentBag<int> hashSet,
    ConcurrentDictionary<ulong, ulong> ouiCounter,
    ConcurrentDictionary<ulong, ulong> regionCounter,
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
        var mData = packet_router_packet_report_v1.Parser.ParseFrom(message);
        var hash = mData.PayloadHash.ToIntHash();
        if (hashSet.Contains<int>(hash))
        {
            dupeCount++;
            continue;
        }

        hashSet.Add(hash);
        messageCount++;
        totalBytes += mData.PayloadSize;
        var u24 = (mData.PayloadSize / 24) + 1;
        dcCount += u24;

        lock (reportLock)
        {
            ulong oui = mData.Oui;
            ulong region = (ulong)mData.Region;
            var mapValue = ouiCounter.GetOrAdd(oui, 0);
            mapValue += u24;
            ouiCounter[oui] = mapValue;

            mapValue = regionCounter.GetOrAdd(region, 0);
            mapValue += u24;
            regionCounter[region] = mapValue;
        }
    }

    var summary = new ReportSummary()
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

    var universalTime = ReportToUnixTime(report);
    Console.WriteLine($"{universalTime.ToShortTimeString()} {report} {summary.ToString()}");
    return summary;
}

static DateTime ReportToUnixTime(string reportName)
{
    var timestamp = Regex.Match(reportName, @"\d+").Value;
    UInt64 microSeconds = UInt64.Parse(timestamp);
    var time = UnixTimeMillisecondsToDateTime(microSeconds);
    return time.ToUniversalTime();
}

static List<(ulong, ulong, float)> ComputeValuePercent(double dcCount, ConcurrentDictionary<ulong, ulong> valueCounter)
{
    List<(ulong, ulong, float)> valuePercentList = new List<(ulong, ulong, float)>();
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
            var regionPercentage = (float)count * 100 / (float)dcCount;
            valuePercentList.Add((value, count, regionPercentage));
        }
    }
    return valuePercentList;
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

static async IAsyncEnumerable<string> ListBucketKeysAsync(IAmazonS3 client, string bucketName, UInt64 unixTime, uint minutes)
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

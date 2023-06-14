using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;
using Google.Protobuf;
using Helium.PacketRouter;
using Merkator.BitCoin;
using System.Collections.Concurrent;
using System.CommandLine;
using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Numerics;
using System.Text.Json;
using System.Text.RegularExpressions;
using static System.Runtime.InteropServices.JavaScript.JSType;

var b58 = "11zKxAVNKnN2UUKbSyFyxvKevjysJDnq1YR1Z9cQCUry5vUWtUo";
var binary0 = Base58Encoding.DecodeWithCheckSum(b58);
var hash0 = Base58Encoding.EncodeWithCheckSum(binary0);

var binary1 = PublicKey.B58ToRawBinary(b58);
var hash1 = PublicKey.PubKeyToB58(binary1);

uint minutes = 60; // 24 * 60;
var startString = ToUnixEpochTime("2023-6-12Z"); // "2023-4-27 12:00:00 AM"// 1680332400;
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

ConcurrentBag<int> uniqueSet = new ConcurrentBag<int>();
ConcurrentDictionary<int, int> freqSet = new ConcurrentDictionary<int, int>();

ConcurrentDictionary<ulong, ulong> ouiCounter = new ConcurrentDictionary<ulong, ulong>();
ConcurrentDictionary<ulong, ulong> regionCounter = new ConcurrentDictionary<ulong, ulong>();
ConcurrentDictionary<ulong, ulong> netIDCounter = new ConcurrentDictionary<ulong, ulong>();
object reportLock = new();

ReportSummary theSummary = new ReportSummary();
List<string> itemList = new List<string>();
ConcurrentDictionary<int, BigInteger> locationMap = new ConcurrentDictionary<int, BigInteger>();

using (var reader = new StreamReader(@"C:/temp/iot_metadata.csv"))
using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
{
    while (csv.Read())
    {
        try
        {
            var record = csv.GetRecord<IOTMetadata>();
            var intHash0 = PublicKey.B58ToRawBinary(record.HotspotKey);
            var intHash1 = BitConverter.ToInt32(intHash0, 0);
            locationMap.TryAdd(intHash1, record.Location.GetValueOrDefault(0));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception {ex}");
        }
    }
}

var list = ListBucketKeysAsync(s3Client, ingestBucket, startUnix, minutes).ToBlockingEnumerable();
var sortedList = list.OrderBy(s => s).ToList();
var last = sortedList.Last();
var listCount = sortedList.Count();
Console.WriteLine($"Starting {ingestBucket} of key size {listCount}");
foreach (var item in sortedList)
{
    Console.WriteLine($"Key {item}");
}

int frMax = 100000;
int[] frArray = new int[frMax];
float[] frPercent = new float[frMax];

foreach (var item in sortedList)
{
    itemList.Add(item);
    if (itemList.Count >= 8 || item == last)
    {
        var taskList = LoopFiles(reportLock, s3Client, uniqueSet, freqSet, ouiCounter, regionCounter, netIDCounter, locationMap, ingestBucket, itemList);
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
        if (uniqueSet.Count > hashSizeMaximum)
        {
            foreach(var keyValue in freqSet)
            {
                frArray[keyValue.Value]++;
            }
            uniqueSet.Clear();
            freqSet.Clear();
        }
    }
}

var burnedDCFees = (theSummary.DCCount) * 0.00001;
stopWatch.Stop();

Console.WriteLine($"Duration is {minutes} minutes");

Console.WriteLine("--------------------------------------");
Console.WriteLine($"Start time is {dateTime.ToUniversalTime()}");
Console.WriteLine($"S3 startAfter file is {startAfter}");
Console.WriteLine($"Duration is {minutes} minutes");
Console.WriteLine($"Elapsed time is {stopWatch.Elapsed}");
Console.WriteLine("--------------------------------------");
Console.WriteLine($"{startUnix} minutes={minutes} loraMsgTotal= {theSummary.MessageCount} dupes= {theSummary.DupeCount} byteTotal= {theSummary.TotalBytes} dcCount= {theSummary.DCCount} fc= {theSummary.FileCount} rawTotal= {(float)theSummary.RawSize / (1024 * 1024)} gzTotal= {(float)theSummary.GzipSize / (1024 * 1024)} fees= {burnedDCFees.ToString("########.##")}");
Console.WriteLine("--------------------------------------");

var metrics = InitMetricsFile(metricsFile, dateTime);

int frTotal = 0;
for (int i = 0; i < frMax; i++)
{
    frTotal += frArray[i];
}

for (int i = 0; i < 10; i++)
{
    float percent = (float)frArray[i] / (float)frTotal;
    frPercent[i] = percent;
    if (percent >= 0.001)
    {
        Console.WriteLine($"Frequency of {i} copy is {(percent * 100).ToString("##.##")}");
    }
}

float redundantPercent = 1.0f - frPercent[0];
var redundantSummary = new RedundantSummary()
{
    Time = dateTime.ToUniversalTime(),
    Percent = redundantPercent,
    Region = (uint)Helium.region.Eu868
};
metrics.RedundantByDay.Add(redundantSummary);

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

List<NetIDSummary>? netIDList = metrics?.NetIDByDay;
var vpList3 = ComputeValuePercent(theSummary.DCCount, netIDCounter);
foreach (var vp in vpList3)
{
    var netIDItem = new NetIDSummary()
    {
        Time = dateTime.ToUniversalTime(),
        NetID = (uint)vp.Item1,
        DCCount = vp.Item2,
        Percent = vp.Item3
    };
    netIDList?.Add(netIDItem);
    Console.WriteLine($"NetID= {vp.Item1} DC={vp.Item2} Percentage= {vp.Item3} %");
};

metrics.LastUpdate = dateTime.ToUniversalTime();
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
            RegionByDay = new List<RegionSummary>(),
            NetIDByDay = new List<NetIDSummary>(),
            RedundantByDay = new List<RedundantSummary>()
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

        var items4 = metrics?.NetIDByDay;
        var toRemove4 = items4?.Where(item => item.Time == dateTime.ToUniversalTime()).ToList();
        foreach (var s in toRemove4)
        {
            items4?.Remove(s);
        }

        var items5 = metrics?.RedundantByDay;
        var toRemove5 = items5?.Where(item => item.Time == dateTime.ToUniversalTime()).ToList();
        foreach (var s in toRemove5)
        {
            items5?.Remove(s);
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
    ConcurrentBag<int> uniqueSet,
    ConcurrentDictionary<int, int> freqSet,
    ConcurrentDictionary<ulong, ulong> ouiCounter,
    ConcurrentDictionary<ulong, ulong> regionCounter,
    ConcurrentDictionary<ulong, ulong> netIDCounter,
    ConcurrentDictionary<int, BigInteger> locationMap,
    string ingestBucket,
    List<string> files)
{
    List<Task<ReportSummary>> taskList = new List<Task<ReportSummary>>();
    foreach (var file in files)
    {
        var summary = GetPacketReportsAsync(reportLock, s3Client, uniqueSet, freqSet, ouiCounter, regionCounter, netIDCounter, locationMap, ingestBucket, file);
        taskList.Add(summary);
    }
    return taskList;
}

static async Task<ReportSummary> GetPacketReportsAsync(
    object reportLock,
    AmazonS3Client s3Client,
    ConcurrentBag<int> uniqueSet,
    ConcurrentDictionary<int, int> freqSet,
    ConcurrentDictionary<ulong, ulong> ouiCounter,
    ConcurrentDictionary<ulong, ulong> regionCounter,
    ConcurrentDictionary<ulong, ulong> netIDCounter,
    ConcurrentDictionary<int, BigInteger> locationMap,
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
        var dataRate = mData.Datarate;
        var gatewayArray = mData.Gateway.ToByteArray();
        var gwArray2 = new byte[] { gatewayArray[0], gatewayArray[1], gatewayArray[2], gatewayArray[3] };

        var gatewayHash0 = BitConverter.ToInt32(gatewayArray, 0);
        var gatewayHash1 = BitConverter.ToInt32(gwArray2, 0);
        var gatewayKey0 = mData.Gateway;
        var gatewayKey1 = gatewayKey0.ToStringUtf8();
        var gatewayKey2 = gatewayKey0.ToBase64();
        var gatewayKey3 = Base58Encoding.Encode(gatewayArray);
        var gatewayKey4 = Base58Encoding.EncodeWithCheckSum(gatewayArray);

        if (locationMap.TryGetValue(gatewayHash0, out var value))
        {
            Console.WriteLine($"Success! value={value}");
        }
        else
        {
            if (locationMap.TryGetValue(gatewayHash1, out var value2))
            {
                Console.WriteLine($"Success! value={value2}");
            }
        }

        var uniqueHash = mData.PayloadHash.ToIntHash();
        //if (mData.NetId == 0x3c)
        //{
        //    Console.WriteLine($"0x3c message");
        //}
        //switch (mData.NetId)
        //{
        //    case 36:
        //    case 9:
        //    case 6291493:
        //    case 12582938:
        //    case 14680096:
        //    case 14680208:
        //    case 6291475:
        //    case 14680099:
        //    case 6291458:
        //    case 12582995:
        //        break;
        //    default:
        //        Console.WriteLine($"NetID={mData.NetId}");
        //        break;
        //}
        //if (mData.Region == Helium.region.Au915Sb1)
        //{
        //    Console.WriteLine($"AS923_1C message");
        //}
        if (mData.Region == Helium.region.Eu868 && mData.Datarate == Helium.data_rate.Sf12Bw125)
        {
            int freqCount = 0;
            if (freqSet.TryGetValue(uniqueHash, out freqCount)) {
                int newValue = freqCount + 1;
                freqSet.TryUpdate(uniqueHash, newValue, freqCount);
            }
            else
            {
                freqSet.TryAdd(uniqueHash, 0);
            }
        }

        if (uniqueSet.Contains(uniqueHash))
        {
            dupeCount++;
            continue;
        }
        else
        {
            uniqueSet.Add(uniqueHash);
        }

        messageCount++;
        totalBytes += mData.PayloadSize;
        var u24 = (mData.PayloadSize / 24) + 1;
        dcCount += u24;

        lock (reportLock)
        {
            ulong oui = mData.Oui;
            ulong region = (ulong)mData.Region;
            ulong netID = (ulong)mData.NetId;

            var mapValue = ouiCounter.GetOrAdd(oui, 0);
            mapValue += u24;
            ouiCounter[oui] = mapValue;

            mapValue = regionCounter.GetOrAdd(region, 0);
            mapValue += u24;
            regionCounter[region] = mapValue;

            mapValue = netIDCounter.GetOrAdd(netID, 0);
            mapValue += u24;
            netIDCounter[netID] = mapValue;
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

global using Amazon;
//global using Amazon.IdentityManagement;
//global using Amazon.IdentityManagement.Model;
global using Amazon.Runtime.Internal;
global using Amazon.Runtime.SharedInterfaces;
global using Microsoft.Extensions.DependencyInjection;
global using Microsoft.Extensions.Hosting;
global using Microsoft.Extensions.Logging;
global using Microsoft.Extensions.Logging.Console;
global using Microsoft.Extensions.Logging.Debug;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Helium.PacketRouter;
using System.Collections.Concurrent;
using System.CommandLine;
using System.Data;
using System.Diagnostics;
using System.Formats.Tar;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;


// Default values
uint minutes = 24 * 60; // 24 * 60;
// var startString = ToUnixEpochTime("2023-7-1Z"); // "2023-4-27 12:00:00 AM"// 1680332400;
var startString = "yesterday";
string ingestBucket = "foundation-iot-packet-ingest";
string metricsBucket = "foundation-iot-metrics";
string metricsKeyName = "iot-metrics.json";
string? metricsFile = @"c:\temp\lorawan_metrics.json";
int hashSizeMaximum = 25000;
bool s3Metrics = false;

if (args.Length > 0)
{
    var startOption = new Option<string?>(
        name: "--starttime",
        description: "The time to start processing files.",
        getDefaultValue: () => "yesterday"
    );
    startOption.AddAlias("-s");

    var durationOption = new Option<uint?>(
        name: "--duration",
        description: "The number of minutes to process, e.g. duration is 60 minutes",
        getDefaultValue: () => 10
    );
    durationOption.AddAlias("-d");

    var outputOption = new Option<string?>(
        name: "--output",
        description: "Output json file",
        getDefaultValue: () => "metrics.json"
    );
    outputOption.AddAlias("-o");

    var s3MetricsOption = new Option<bool>(
        name: "--s3metrics",
        description: "Enable s3 metrics output"
    );

    var rootCommand = new RootCommand("IngestParser");
    rootCommand.AddOption(startOption);
    rootCommand.AddOption(durationOption);
    rootCommand.AddOption(outputOption);
    rootCommand.AddOption(s3MetricsOption);

    rootCommand.SetHandler((inStartTime, inMinutes, output, s3MetricsFlag) =>
    {
        startString = ParseUnixEpochTime(inStartTime).ToString();
        minutes = inMinutes.GetValueOrDefault(10);
        metricsFile = output;
        s3Metrics = s3MetricsFlag;
    }, startOption, durationOption, outputOption, s3MetricsOption);

    var cmd = await rootCommand.InvokeAsync(args);
    Console.WriteLine($"cmd={cmd}");
}
else
{
    startString = ParseUnixEpochTime(startString).ToString();
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
Console.WriteLine($"S3Metrics is {s3Metrics}");

// Create an S3 client object.
AmazonS3Client? s3Client = null;
var ic = FetchEnvironmentCredentials();
if (ic is null)
{
    Console.WriteLine("Begin sequence to call AmazonS3Client");

    string tokenFile = @"/var/run/secrets/eks.amazonaws.com/serviceaccount/token";

    if (File.Exists(tokenFile))
    {
        // Open the text file using a stream reader.
        Console.WriteLine($"Reading token at {tokenFile}");
        using (var sr = new StreamReader(tokenFile))
        {
            // Read the stream as a string, and write the string to the console.
            Console.WriteLine(sr.ReadToEnd());
        }
    }
    else
    {
        Console.WriteLine($"Token does not exist at {tokenFile}");
    }

    await HelloIAM();

    var iamClient = new AmazonIdentityManagementServiceClient();

    var listPoliciesPaginator = iamClient.Paginators.ListPolicies(new ListPoliciesRequest());
    var policies = new List<ManagedPolicy>();

    IAmazonIdentityManagementService _IAMService = iamClient;
    var listRolesPaginator = _IAMService.Paginators.ListRoles(new ListRolesRequest());
    var roles = new List<Role>();

    await foreach (var response in listRolesPaginator.Responses)
    {
        roles.AddRange(response.Roles);
    }

    foreach(var role in roles)
    {
        Console.WriteLine($"Role is {role.Arn}");
    }

    Console.WriteLine("Calling AmazonS3Client()");
    s3Client = new AmazonS3Client();
}
else
{
    s3Client = new AmazonS3Client(ic.AccessKey, ic.SecretKey, ic.Token, RegionEndpoint.USWest2);
}

if (s3Metrics)
{
    var metricsResponse = await DownloadS3Object(s3Client, metricsBucket, metricsKeyName, false);
    var metricsData = metricsResponse.Item4;
    metricsFile = Encoding.UTF8.GetString(metricsData, 0, metricsData.Length);
    Console.WriteLine($"S3 metrics bucket={metricsBucket} key={metricsKeyName}");
}

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

var list = ListBucketKeysAsync(s3Client, ingestBucket, startUnix, minutes).ToBlockingEnumerable();
var sortedList = list.OrderBy(s => s).ToList();
if (!sortedList.Any())
{
    Console.WriteLine($"No bucket keys found for {ingestBucket} {startUnix}");
    return;
}
var last = sortedList.Last();
var listCount = sortedList.Count();
Console.WriteLine($"Starting {ingestBucket} of key size {listCount} for {dateTime.ToUniversalTime().ToShortDateString()}");
//foreach (var item in sortedList)
//{
//    Console.WriteLine($"Key {item}");
//}

int frMax = 100000;
int[] frArray = new int[frMax];
float[] frPercent = new float[frMax];

foreach (var item in sortedList)
{
    itemList.Add(item);
    if (itemList.Count >= 8 || item == last)
    {
        var taskList = LoopFiles(reportLock, s3Client, uniqueSet, freqSet, ouiCounter, regionCounter, netIDCounter, ingestBucket, itemList);
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

var metrics = InitMetricsFile(metricsFile, dateTime, s3Metrics);

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
var metricsJson = GenerateMetricsJson(metrics, theSummary, dateTime, minutes);
if (s3Metrics)
{
    await UploadToS3Async(s3Client, metricsBucket, metricsKeyName, metricsJson);
}
else
{
    File.WriteAllText(metricsFile, metricsJson);
}
return;

static async Task HelloIAM()
{
    // Getting started with AWS Identity and Access Management (IAM). List
    // the policies for the account.
    var iamClient = new AmazonIdentityManagementServiceClient();

    var listPoliciesPaginator = iamClient.Paginators.ListPolicies(new ListPoliciesRequest());
    var policies = new List<ManagedPolicy>();

    await foreach (var response in listPoliciesPaginator.Responses)
    {
        policies.AddRange(response.Policies);
    }

    Console.WriteLine("Here are the policies defined for your account:\n");
    policies.ForEach(policy =>
    {
        Console.WriteLine($"Created: {policy.CreateDate}\t{policy.PolicyName}\t{policy.Description}");
    });
}

static ImmutableCredentials? FetchEnvironmentCredentials()
{
    string accessKeyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY");
    string secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
    string sessionToken = Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN");

    if (string.IsNullOrEmpty(accessKeyId) || string.IsNullOrEmpty(secretKey))
    {
        Console.WriteLine("The environment variables were not set with AWS credentials.");
        return null;
    }
    else
    {
        Console.WriteLine("AWS Credentials found using environment variables.");
        return new ImmutableCredentials(accessKeyId, secretKey, sessionToken);
    } 
}

static LoRaWANMetrics? InitMetricsFile(string metricsFile, DateTime dateTime, bool s3Metrics)
{
    //Console.WriteLine($"InitMetricsFile s3Metrics={s3Metrics} name={metricsFile}");
    if (!s3Metrics && !File.Exists(metricsFile))
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
        string? jsonMetrics = String.Empty;
        if (s3Metrics)
        {
            jsonMetrics = metricsFile;
        }
        else
        {
            File.ReadAllText(metricsFile);
        }

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
static string? GenerateMetricsJson(LoRaWANMetrics? metrics, ReportSummary report, DateTime dateTime, uint duration)
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

    return jsonMetrics;
}

static List<Task<ReportSummary>> LoopFiles(
    object reportLock,
    AmazonS3Client s3Client,
    ConcurrentBag<int> uniqueSet,
    ConcurrentDictionary<int, int> freqSet,
    ConcurrentDictionary<ulong, ulong> ouiCounter,
    ConcurrentDictionary<ulong, ulong> regionCounter,
    ConcurrentDictionary<ulong, ulong> netIDCounter,
    string ingestBucket,
    List<string> files)
{
    List<Task<ReportSummary>> taskList = new List<Task<ReportSummary>>();
    foreach (var file in files)
    {
        var summary = GetPacketReportsAsync(reportLock, s3Client, uniqueSet, freqSet, ouiCounter, regionCounter, netIDCounter, ingestBucket, file);
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

        var uniqueHash = mData.PayloadHash.ToIntHash();

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
static string YesterdayUTC()
{
    string yesterday = DateTime.Now.AddDays(-1).ToUniversalTime().ToString("MM-dd-yyyyZ");
    return yesterday;
}

static long ParseUnixEpochTime(string textTime)
{
    if (textTime.ToLower() == "yesterday")
    {
        textTime = YesterdayUTC();
    }

    var time2 = textTime?.TrimEnd('Z') + 'Z';
    var epochTime = ToUnixEpochTime(time2);
    return epochTime;
}

static long ToUnixEpochTime(string textDateTime)
{
    // textString "2023-4-12 2:27:01 PM"
    var dateTime = DateTime.Parse(textDateTime).ToUniversalTime();

    DateTimeOffset dto = new DateTimeOffset(dateTime);
    return dto.ToUnixTimeSeconds();
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

static async Task<bool> UploadToS3Async(
    IAmazonS3 client,
    string bucketName,
    string objectName,
    string objectContent)
{
    var request = new PutObjectRequest
    {
        BucketName = bucketName,
        Key = objectName,
        ContentBody = objectContent,
    };

    var response = await client.PutObjectAsync(request);
    if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
    {
        Console.WriteLine($"Successfully uploaded {objectName} to {bucketName}.");
        return true;
    }
    else
    {
        Console.WriteLine($"Could not upload {objectName} to {bucketName}.");
        return false;
    }
}

static async Task<(DateTime, ulong, ulong, byte[])> DownloadS3Object(AmazonS3Client s3Client, string bucketName, string keyName, bool gzip = true)
{
    var getObjectResult = await s3Client.GetObjectAsync(bucketName, keyName);
    var modTime = getObjectResult.LastModified.ToUniversalTime();
    var gzipSize = getObjectResult.Headers.ContentLength;
    using var goStream = getObjectResult.ResponseStream;

    using MemoryStream memoryStream = new MemoryStream();

    goStream.CopyTo(memoryStream);

    memoryStream.Position = 0;
    memoryStream.Seek(0, SeekOrigin.Begin);

    byte[]? unzip = null;
    if (!gzip)
    {
        unzip = memoryStream.ToArray();
    }
    else
    {
        unzip = DecompressSteam(memoryStream);
    }
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

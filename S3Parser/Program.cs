// See https://aka.ms/new-console-template for more information
using Google.Protobuf;
using Helium.PacketRouter;
using ParquetSharp.IO;
using ParquetSharp;
using ParquetSharp.RowOriented;
using System;
using System.IO.Compression;
using System.Collections.Immutable;

Console.WriteLine("Hello, World!");

//string path = @"c:\temp\MyTest.txt";
string path = @"c:\temp\packetreport.1666990455151.gz";

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

        //HashSet<int> FreqSet= new HashSet<int>();
        //HashSet<string>

        var row = PopulateParquetRow(message);
        rowWriter.WriteRow(row);
        var rows2 = rowWriter.FileMetaData?.NumRows;
        Console.WriteLine($"rows = {rows2}");

        data = data.Skip(4 + m_size).ToArray();

    } while (true);

    rowWriter.Close();
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


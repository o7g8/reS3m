# reS3m ("restream")

The command-line utility allows to consolidate chunked data stored in S3 into a continious data stream for further processing.

The utility reads names of S3 objects from `stdin`, reads the objects data from S3 and sends it into `stdout`. The utility prints diagnostic messages into `stderr`.

One of the use cases is the restreaming of AWS Ground Station DigIF data persisted in S3 into a SDR.

The utility has following optional command-line arguments:

* `-w` - amount of workers (default: 10)

* `-c` - chunk (buffer) size for each worker in bytes (default: 8388608 = 8 Mb)

* `-s` - skip bytes from the start each object, can be used to skip object headers (default: 0)

To achive maximum performance in your environment (data, machine, network), you need to experiment with values of `-w` and `-c`. The "good" value of `-c` for large objects is normally within 8-16 Mb.

If you use the utility within a VPC, then access S3 via a private endpoint for the maximum performance.

For maximum performance you need to build the utility as a native binary as described below in the document.

## Examples

Download the object `s3://<bucket>/<object>` using 10 workers each reading chunks of 8Mb and save it into `file`:

```bash
echo s3://<bucket>/<object> | reS3m -w 10 -c 8388608 >file 2>debug.log
```

Stream the S3 objects listed in `object_list`, using 15 workers each reading chunks of 8Mb, via TCP using NetCat:

```bash
cat object_list | reS3m -w 15 -c 8388608 | nc localhost 5001 
```

Re-stream AWS Groud Station DigIF data stripping 24 bytes of PCAP header from each objects and using 60 workers with 16Mb buffer each:

```bash
cat s3objects.txt | ~/bin/reS3m -s 24 -w 60 -c 16777216 2>debug.log | <further processing>
```

## Build

To build the utility you need to have [.NET 7 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/7.0) installed on the build machine. You can build the utility on Linux, Windows, and macOS.

Build self-contained executable for Linux/x64:

```bash
dotnet publish -r linux-x64 -p:PublishSingleFile=true -c Release --self-contained true
```

for Linux/ARM64:

```bash
dotnet publish -r linux-arm64 -p:PublishSingleFile=true -c Release --self-contained true
```

for Windows:

```bash
dotnet publish -r win-x64 -p:PublishSingleFile=true -c Release --self-contained true
```

for Mac OS X/x64:

```bash
dotnet publish -r osx.12-x64 -p:PublishSingleFile=true -c Release --self-contained true
```

See more about Runtime Identifiers (RIDs) in <https://learn.microsoft.com/en-us/dotnet/core/rid-catalog>

### Dependencies

The tool uses [Akka.NET](https://getakka.net/) and [AWS SDK for .NET](https://aws.amazon.com/sdk-for-net/).

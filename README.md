# README

Download the object `s3://<bucket>/<object>` using 10 workers each reading chunks of 8Mb and save it into `file`:

```bash
echo s3://<bucket>/<object> | reS3m -w 10 -c 8388608 >file 2>debug
```

Stream the S3 objects listed in `object_list`, using 15 workers each reading chunks of 8Mb, via TCP using NetCat:

```bash
cat object_list | reS3m -w 15 -c 8388608 | nc localhost 5001 
```

## Build

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

## References

* <https://petabridge.com/blog/async-await-vs-pipeto/>

* <https://getakka.net/articles/actors/coordinated-shutdown.html>

* <https://github.com/akkadotnet/akka.net/discussions/6271>
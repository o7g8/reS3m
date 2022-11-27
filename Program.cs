using Amazon.S3;
using Amazon.S3.Transfer;

// TODO: add parameters
// - no of workers
// - chunk size
// - Akka.NET <https://petabridge.com/blog/async-await-vs-pipeto/>

var chunkSize = 100;
var buffer = new byte[chunkSize];

using var s3client = new Amazon.S3.AmazonS3Client();
using var stdout = Console.OpenStandardOutput();

var s3ObjName = Console.In.ReadLine();
//var s3ObjName = "s3://s3-frankfurt-1010101/import_template.csv";
while(s3ObjName != null) {
    (var bucketName, var key) = ParseS3Uri(s3ObjName);

    var meta = await s3client.GetObjectMetadataAsync(bucketName, key);
    var chunks = GetChunks(meta.ContentLength, chunkSize);
    
    /*
    Console.Error.WriteLine($"{meta.ContentLength}");
    foreach (var chunk in chunks)
    {
        Console.Error.WriteLine($"{chunk.No}, {chunk.Start}, {chunk.End}, {chunk.Size}");
    }
    System.Environment.Exit(0);
    */

    foreach (var chunk in chunks) {
        Console.Error.WriteLine($"--> {s3ObjName} #{chunk.No}({chunk.Size})[{chunk.Start},{chunk.End}]");
        var readLength = await ReadChunk(buffer, bucketName, key, chunk);

        Console.Error.WriteLine($"<-- {s3ObjName} #{chunk.No}({readLength})");
        FlushChunk(buffer, readLength);
    }

    s3ObjName = Console.In.ReadLine();
}

IEnumerable<Chunk> GetChunks(long objSize, int chunkSize) {
    var chunks = new List<Chunk>();
    for(int start = 0, no = 0; start <= objSize; start = start + chunkSize, no++) {
        chunks.Add(new Chunk {
            No = no,
            Start = start,
            End = Math.Min(objSize - 1, start + chunkSize - 1),
        });
    }
    return chunks;
}

void FlushChunk(byte[] buffer, int length) {
    stdout.Write(buffer, 0, length);
    stdout.Flush();
}

async Task<int> ReadChunk(byte[] buffer, string bucketName, string key, Chunk chunk) {
    //var range = new Amazon.S3.Model.ByteRange(chunkNo * chunkSize, (chunkNo + 1) * chunkSize - 1);
    var range = new Amazon.S3.Model.ByteRange(chunk.Start, chunk.End);
    var request = new Amazon.S3.Model.GetObjectRequest() {
        BucketName = bucketName,
        Key = key,
        ByteRange = range
    };
    var resp = await s3client.GetObjectAsync(request);
    using var stream = resp.ResponseStream;
    return stream.Read(buffer, 0, chunkSize);
}

(string bucketName, string key) ParseS3Uri(string s3uri) {
    var uri = new System.Uri(s3uri);
    var key = uri.AbsolutePath.Substring(1); // remove 1st slash
    return (uri.Host, key);
}

public struct Chunk {
    public int No;
    public long Start;
    public long End;
    public long Size => End - Start + 1;

/*
    public override string ToString() {
        return $"No={No} Start={Start} End={End} Size={Size}";
    }
    */
}
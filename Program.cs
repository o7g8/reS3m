using Amazon.S3;
using Amazon.S3.Transfer;

// TODO: add parameters
// - no of workers
// - chunk size

uint chunkNo = 0;
var chunkSize = 10;

using var s3client = new Amazon.S3.AmazonS3Client();
using var stdout = Console.OpenStandardOutput();


var s3ObjName = Console.In.ReadLine();
while(s3ObjName != null) {
    Console.Error.WriteLine($"--> {s3ObjName} #{chunkNo}({chunkSize})");
    (var bucketName, var key) = ParseS3Uri(s3ObjName);

    var buffer = new byte[chunkSize];
    var readLength = await ReadChunk(buffer, bucketName, key, chunkNo, chunkSize);

    Console.Error.WriteLine($"<-- {s3ObjName} #{chunkNo}({readLength})");
    FlushChunk(buffer, readLength);

    s3ObjName = Console.In.ReadLine();
}

void FlushChunk(byte[] buffer, int length) {
    stdout.Write(buffer, 0, length);
    stdout.Flush();
}

async Task<int> ReadChunk(byte[] buffer, string bucketName, string key, uint chunkNo, int chunkSize) {
    var range = new Amazon.S3.Model.ByteRange(chunkNo * chunkSize, (chunkNo + 1) * chunkSize - 1);
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

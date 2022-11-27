using Akka.Actor;
using Amazon.S3;
using Amazon.S3.Transfer;
using reS3m;

// TODO: add parameters
// - no of workers
// - chunk size
// - Akka.NET <https://petabridge.com/blog/async-await-vs-pipeto/>

var workers = 3;
var chunkSize = 100;
var buffer = new byte[chunkSize];

using var s3client = new Amazon.S3.AmazonS3Client();
using var stdout = Console.OpenStandardOutput();

var actorSystem = ActorSystem.Create("reS3m");
var manager = actorSystem.ActorOf(Props.Create<Manager>(s3client, stdout, workers, chunkSize), "manager");


var s3ObjName = Console.In.ReadLine();
//var s3ObjName = "s3://s3-frankfurt-1010101/import_template.csv";
while(s3ObjName != null) {
    (var bucketName, var key) = ParseS3Uri(s3ObjName);

    var meta = await s3client.GetObjectMetadataAsync(bucketName, key);
    manager.Tell(new reS3m.Messages.DownloadObject(bucketName, key, meta.ContentLength));

    s3ObjName = Console.In.ReadLine();
}

//Console.Read();
//Console.In.ReadLine();
Thread.Sleep(100000);
await actorSystem.Terminate();


(string bucketName, string key) ParseS3Uri(string s3uri) {
    var uri = new System.Uri(s3uri);
    var key = uri.AbsolutePath.Substring(1); // remove 1st slash
    return (uri.Host, key);
}



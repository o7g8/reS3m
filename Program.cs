using Akka.Actor;
using reS3m;

// TODO: add CLI arguments
// -w: no of workers
// -c: chunk size // does it need to have alignment (e.g. like x1024)???

var workers = 3;
var chunkSize = 100;

using var allWorkDone = new Barrier(2);
using var s3client = new Amazon.S3.AmazonS3Client();
using var stdout = Console.OpenStandardOutput();

using var actorSystem = ActorSystem.Create("reS3m");
var manager = actorSystem.ActorOf(Props.Create<Manager>(s3client, stdout, workers, chunkSize, allWorkDone), "manager");

var s3ObjName = Console.In.ReadLine();
while(s3ObjName != null) {
    (var bucketName, var key) = ParseS3Uri(s3ObjName);

    var meta = await s3client.GetObjectMetadataAsync(bucketName, key);
    manager.Tell(new reS3m.Messages.DownloadObject(bucketName, key, meta.ContentLength));

    s3ObjName = Console.In.ReadLine();
}

manager.Tell(new reS3m.Messages.NoMoreWork());
allWorkDone.SignalAndWait();

(string bucketName, string key) ParseS3Uri(string s3uri) {
    var uri = new System.Uri(s3uri);
    var key = uri.AbsolutePath.Substring(1); // remove 1st slash
    return (uri.Host, key);
}

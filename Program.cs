using Akka.Actor;
using CommandLine;
using reS3m;

var workers = 10;
var chunkSize = 8 * 1024 * 1024;
var skipBytes = 0;

Parser.Default.ParseArguments<CommandLineOptions>(args)
      .WithParsed<CommandLineOptions>(o => {
           workers = o.Workers;
           chunkSize = o.ChunkSize;
           skipBytes = o.SkipBytes;
      })
      .WithNotParsed<CommandLineOptions>(o => {
           System.Environment.Exit(0);
      });

using var allWorkDone = new Barrier(2);
using var s3client = new Amazon.S3.AmazonS3Client();
using var stdout = Console.OpenStandardOutput();

using var actorSystem = ActorSystem.Create("reS3m");
var manager = actorSystem.ActorOf(Props.Create<Manager>(s3client, stdout, workers, chunkSize, allWorkDone), "manager");

var s3ObjName = Console.In.ReadLine();
while(s3ObjName != null) {
    (var bucketName, var key) = ParseS3Uri(s3ObjName);

    var meta = await s3client.GetObjectMetadataAsync(bucketName, key);
    manager.Tell(new reS3m.Messages.DownloadObject(bucketName, key, meta.ContentLength, skipBytes));

    s3ObjName = Console.In.ReadLine();
}

manager.Tell(new reS3m.Messages.NoMoreWork());
allWorkDone.SignalAndWait();

(string bucketName, string key) ParseS3Uri(string s3uri) {
    var uri = new System.Uri(s3uri);
    var key = uri.AbsolutePath.Substring(1); // remove the 1st slash
    return (uri.Host, key);
}

public class CommandLineOptions
{
   [Option('w', "workers", Required = false, Default = 10,  HelpText = "Number of workers")]
   public int Workers { get; set; }
 
   [Option('c', "chunk-size", Required = false, Default = 8 * 1024 * 1024, HelpText = "Chunk size in bytes" )]
   public int ChunkSize { get; set; }

   [Option('s', "skip-bytes", Required = false, Default = 0, HelpText = "Skip the first N bytes of each S3 object" )]
   public int SkipBytes { get; set; }
}

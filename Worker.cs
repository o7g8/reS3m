using Akka.Actor;
using Amazon.S3;
using Amazon.S3.Model;
using reS3m.Messages;

namespace reS3m {
    internal class ChunkDownloader : ReceiveActor {
        private readonly IActorRef manager;
        private readonly AmazonS3Client s3;
        private readonly int chunkSize;
        private readonly Stream stdout;
        private readonly byte[] buffer;
        private int downloadedBytes;
        private string downloadedS3Obj = string.Empty;
        private long downloadedChunkNo;
        
        public ChunkDownloader(IActorRef manager, AmazonS3Client s3client, int chunkSize, Stream stdout) {
            this.manager = manager;
            this.s3 = s3client;
            this.chunkSize = chunkSize;
            this.stdout = stdout;
            this.buffer = new byte[chunkSize];

            ReceiveAsync<Messages.DownloadChunk>(chunk => DownloadChunk(chunk));
            Receive<Messages.FlushChunk>(chunk => Flush(chunk));
        }

        private async Task DownloadChunk(DownloadChunk chunk)
        {
            Log($"W: ---> {chunk.BucketName}/{chunk.Key} #{chunk.Chunk.No}[{chunk.Chunk.Start},{chunk.Chunk.End}]");
            var request = new GetObjectRequest() {
                BucketName = chunk.BucketName,
                Key = chunk.Key,
                ByteRange = new ByteRange(chunk.Chunk.Start, chunk.Chunk.End)
            };
            
            // TODO: consider incremental download, so we don't waste already downloaded data
            for(var attempt = 0; attempt < 5; attempt++) {
                try {
                    using var resp = await s3.GetObjectAsync(request);
                    using var stream = resp.ResponseStream;
                    downloadedBytes = stream.Read(buffer, 0, chunkSize);
                } catch(Exception e) {
                    Log($"W: ERROR - {e.Message} (try {attempt})");
                    continue;
                    //throw;
                }

                if(downloadedBytes != chunk.Chunk.Size) {
                    Log($"W: ERROR - Downloaded {downloadedBytes} bytes instead of {chunk.Chunk.Size} (try {attempt})");
                    continue;
                    //throw new Exception($"Downloaded {downloadedBytes} bytes instead of {chunk.Chunk.Size}");
                }
                break;   
            }

            
            downloadedChunkNo = chunk.Chunk.No;
            downloadedS3Obj = chunk.BucketName + "/" + chunk.Key;
            manager.Tell(new ChunkDownloaded(Self, chunk));
        }

        private void Flush(Messages.FlushChunk chunk) {
            Log($"W: <--- {chunk.S3ObjectName} #{chunk.ChunkNo}");

            if(downloadedS3Obj != chunk.S3ObjectName || downloadedChunkNo != chunk.ChunkNo) {
                Log("W: ERROR - Flushing wrong chunk");
                throw new Exception("Flushing wrong chunk");
            }
            stdout.Write(buffer, 0, downloadedBytes);
            stdout.Flush();
            manager.Tell(new Messages.ChunkFlushed(Self, chunk.S3ObjectName, chunk.ChunkNo));
        }

        private void Log(string log) {
            Console.Error.WriteLine($"{Self.Path}: {log}");
        }
    }
}
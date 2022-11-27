
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
        private int downloadedChunkNo;
        
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
            Log($"W: ---> {chunk.BucketName}/{chunk.Key} #{chunk.Chunk.No}");
            var request = new GetObjectRequest() {
                BucketName = chunk.BucketName,
                Key = chunk.Key,
                ByteRange = new ByteRange(chunk.Chunk.Start, chunk.Chunk.End)
            };
            var resp = await s3.GetObjectAsync(request);
            using var stream = resp.ResponseStream;
            downloadedBytes = stream.Read(buffer, 0, chunkSize);
            if(downloadedBytes != chunk.Chunk.Size) {
                throw new Exception($"Downloaded {downloadedBytes} bytes instead of {chunk.Chunk.Size}");
            }
            downloadedChunkNo = chunk.Chunk.No;
            downloadedS3Obj = chunk.BucketName + "/" + chunk.Key;
            manager.Tell(new ChunkDownloaded(Self, chunk));
        }

        private void Flush(Messages.FlushChunk chunk) {
            Log($"W: <--- {chunk.S3ObjectName} #{chunk.ChunkNo}");

            if(downloadedS3Obj != chunk.S3ObjectName || downloadedChunkNo != chunk.ChunkNo) {
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
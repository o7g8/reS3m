using Akka.Actor;
using Amazon.S3;
using Amazon.S3.Model;
using reS3m.Messages;

namespace reS3m {
    internal class ChunkDownloader : ReceiveActor {
        const int DOWNLOAD_ATTEMPTS = 10;
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
            var expectedLength = chunk.Chunk.End - chunk.Chunk.Start + 1;
            
            int attempt = 0;
            for(attempt = 0; attempt < DOWNLOAD_ATTEMPTS; attempt++) {
                try {
                    using var resp = await s3.GetObjectAsync(request);
                    if(resp.ContentLength != expectedLength) {
                        Log($"W: ERROR - chunk #{chunk.Chunk.No} expected {expectedLength} actual content length {resp.ContentLength} (try {attempt})");
                        continue;
                    }
                    using var responseStream = resp.ResponseStream;
                    using var stream = new BufferedStream(responseStream);
                    var offset = 0;
                    var bytesToRead = (int)expectedLength;
                    while(bytesToRead > 0) {
                        var bytesRead = stream.Read(buffer, offset, bytesToRead);
                        bytesToRead = bytesToRead - bytesRead;
                        offset = offset + bytesRead;
                    }
                    downloadedBytes = (int)expectedLength;
                } catch(Exception e) {
                    Log($"W: ERROR - {e.Message} (try {attempt})");
                    continue;
                    //throw;
                }

                if(downloadedBytes != chunk.Chunk.Size) {
                    Log($"W: ERROR - chunk #{chunk.Chunk.No} downloaded {downloadedBytes} bytes instead of {chunk.Chunk.Size} (try {attempt})");
                    continue;
                    //throw new Exception($"Downloaded {downloadedBytes} bytes instead of {chunk.Chunk.Size}");
                }
                break;   
            }
            if(attempt == DOWNLOAD_ATTEMPTS) {
                Log($"W: ERROR - failed to download chunk #{chunk.Chunk.No}");
                throw new Exception("Failed to download chunk #{chunk.Chunk.No}");
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
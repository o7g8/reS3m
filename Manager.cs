using Akka.Actor;
using Amazon.S3;
using reS3m.Messages;

namespace reS3m {
    internal class Manager : ReceiveActor {
        private readonly AmazonS3Client s3;
        private readonly Stream stdout;
        private readonly int chunkSize;
        private readonly Barrier allWorkDone;

        private readonly List<IActorRef> Workers = new List<IActorRef>();
        private readonly HashSet<IActorRef> FreeWorkers = new HashSet<IActorRef>();

        private readonly Queue<DownloadJob> DownloadQueue = new Queue<DownloadJob>();
        // s3Obj -> chunk
        private readonly Queue<FlushJob> FlushQueue = new Queue<FlushJob>();

        // objName -> {chunkNo}
        private Dictionary<string, Dictionary<int, IActorRef>> DownloadedChunks = 
            new Dictionary<string, Dictionary<int, IActorRef>>();

        private bool noMoreWork = false;

        public Manager(AmazonS3Client s3client, Stream stdout, int workers, int chunkSize, Barrier allWorkDone) {
            this.s3 = s3client;
            this.stdout = stdout;
            this.chunkSize = chunkSize;
            this.allWorkDone = allWorkDone;

            CreateWorkers(workers);
            Receive<Messages.DownloadObject>(s3object => DownloadObject(s3object));
            Receive<Messages.NoMoreWork>(noWork => NoMoreWork(noWork));
            Receive<Messages.ChunkDownloaded>(chunk => ChunkDownloaded(chunk));
            Receive<Messages.ChunkFlushed>(ready => WorkerReady(ready));
        }

        private void DownloadObject(Messages.DownloadObject obj) {
            var chunks = GetChunks(obj.Size, chunkSize);
            Log($"M: {obj.Bucket}/{obj.Key} is split into {chunks.Count} chunks.");
            foreach (var chunk in chunks) {
                var job = new DownloadJob {
                    Bucket = obj.Bucket,
                    Key = obj.Key,
                    Chunk = chunk};
                DownloadQueue.Enqueue(job);
                FlushQueue.Enqueue(new FlushJob {
                    S3ObjectName = FullObjName(job.Bucket, job.Key),
                    ChunkNo = job.Chunk.No
                });
            }
            SendDownloadWork();
        }

        private string FullObjName(string bucket, string key) {
            return bucket + "/" + key; 
        }

        private void SendDownloadWork() {
            var freeWorkers = new List<IActorRef>(FreeWorkers);
            foreach (var worker in freeWorkers)
            {
                if(DownloadQueue.Any()) {
                    var job = DownloadQueue.Dequeue();
                    worker.Tell(new Messages.DownloadChunk(job.Bucket, job.Key, job.Chunk));
                    FreeWorkers.Remove(worker);
                }
            }
        }

        private List<Chunk> GetChunks(long objSize, int chunkSize) {
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

        private void NoMoreWork(Messages.NoMoreWork noWork) {
            this.noMoreWork = true;
        }

        private void CreateWorkers(int workers) {
            for(var i = 0; i < workers; i++) {
                var worker = Context.ActorOf(Props.Create<ChunkDownloader>(Self, s3, chunkSize, stdout), $"worker{i}");
                Workers.Add(worker);
                FreeWorkers.Add(worker);
                Console.Error.WriteLine($"Created worker{i}");
            }
        }

        private void SendFlushWork() {
            if(!FlushQueue.Any()) {
                Log("M: No more work");
                allWorkDone.SignalAndWait();
                return;
            }
            var flushJob = FlushQueue.Peek();
            if(!DownloadedChunks.ContainsKey(flushJob.S3ObjectName)) {
                return; // no chunks for the given object is downloaded
            } 
            if(!DownloadedChunks[flushJob.S3ObjectName].ContainsKey(flushJob.ChunkNo)) {
                return; // the chunk is not yet downloaded
            }
            var worker = DownloadedChunks[flushJob.S3ObjectName][flushJob.ChunkNo];
            worker.Tell(new Messages.FlushChunk(flushJob.S3ObjectName, flushJob.ChunkNo));

            // remove the flushed chunk from downloaded
            DownloadedChunks[flushJob.S3ObjectName].Remove(flushJob.ChunkNo);
            if(!DownloadedChunks[flushJob.S3ObjectName].Any()) {
                DownloadedChunks.Remove(flushJob.S3ObjectName);
            }
        }

        private void ChunkDownloaded(Messages.ChunkDownloaded chunk) {
            // Once the chunk is downloaded we need to reconcile it with the
            var s3obj = FullObjName(chunk.Chunk.BucketName, chunk.Chunk.Key);
            if(DownloadedChunks.ContainsKey(s3obj)) {
                DownloadedChunks[s3obj].Add(chunk.Chunk.Chunk.No, chunk.Sender);
            } else {
                DownloadedChunks.Add(s3obj, new Dictionary<int, IActorRef> {
                    {chunk.Chunk.Chunk.No, chunk.Sender}
                });
            }
            SendFlushWork();
        }

        private void WorkerReady(Messages.ChunkFlushed flushed) {
            FreeWorkers.Add(flushed.Actor);
            SendDownloadWork();

            DequeueFlushedChunk(flushed);
            SendFlushWork();
        }

        private void DequeueFlushedChunk(ChunkFlushed flushed)
        {
            if(!FlushQueue.Any()) {
                throw new Exception("Expected non-empty flush queue");
            }
            var queued = FlushQueue.Dequeue();
            if(queued.S3ObjectName != flushed.S3ObjectName || 
                queued.ChunkNo != flushed.ChunkNo) {
                throw new Exception("Mismatch b/w flushed and queued to flush");
            }
        }

        private void Log(string log) {
            Console.Error.WriteLine($"{Self.Path}: {log}");
        }
    }

    public struct FlushJob {
        public string S3ObjectName;
        public int ChunkNo;
    }
    public struct DownloadJob {
        public string Bucket;
        public string Key;
        public Chunk Chunk;
    }
}
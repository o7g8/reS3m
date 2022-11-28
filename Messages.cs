using Akka.Actor;

namespace reS3m.Messages
{
    public struct Chunk {
        public long No;
        public long Start;
        public long End;
        public long Size => End - Start + 1;
    }

    public class DownloadObject {
        public string Bucket {get; }
        public string Key {get; }
        public long Size {get;}

        public DownloadObject(string bucket, string key, long size)
        {
            this.Bucket = bucket;
            this.Key = key;
            this.Size = size; 
        } 
    }

    public class NoMoreWork {

    }

    public class ChunkFlushed {
        public IActorRef Actor {get; }
        public string S3ObjectName {get;}
        public long ChunkNo {get;}

        public ChunkFlushed (IActorRef actor, string s3obj, long chunkNo) {
            Actor = actor;
            S3ObjectName = s3obj;
            ChunkNo = chunkNo;
        } 
    }

    public class DownloadChunk {
        public Chunk Chunk {get; set;}
        public string BucketName {get; set; }
        public string Key {get; set; }

        public DownloadChunk(string bucketName, string key, Chunk chunk) {
            BucketName = bucketName;
            Key = key;
            Chunk = chunk;
        }
    }


    public class ChunkDownloaded {
        public DownloadChunk Chunk {get; set;} 
        public IActorRef Sender {get; set; }

        public ChunkDownloaded(IActorRef sender, DownloadChunk chunk) {
            Sender = sender;
            Chunk = chunk;
        }
    }

    public class FlushChunk {
        public string S3ObjectName {get;}
        public long ChunkNo {get;}

        public FlushChunk(string s3ObjectName, long chunkNo)
        {
            S3ObjectName = s3ObjectName;
            ChunkNo = chunkNo;
        }
    }
}
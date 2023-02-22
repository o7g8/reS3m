using Amazon.S3;

namespace reS3m
{
    public interface IS3ClientFactory {
        public IAmazonS3 CreateClient();
    }

    public class S3ClientFactory : IS3ClientFactory {
        public IAmazonS3 CreateClient() {
            return new Amazon.S3.AmazonS3Client();
        }
    }
}
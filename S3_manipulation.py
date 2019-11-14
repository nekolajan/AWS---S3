import boto3
from boto3.session import Session

class aws_s3:
    """" AWS S3 """

    def __init__(self, access_key, secret_key, bucket, object_path):

        """ AWS credentials """
        
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.object_path = object_path

    def s3_connection(self):
        """ Connection to S3 """
        try:
            self.session = Session( aws_access_key_id = self.access_key,
                                        aws_secret_access_key = self.secret_key)
            self.s3 = self.session.resource('s3')
            self.bckt = self.s3.Bucket(self.bucket)
            self.bucket_name = self.bckt.name
            print('Connected to S3.')
        except:
            print('Not connected to S3')
            
    def delete_object(self):
        """ Delete Object in S3 """
        
        self.s3 = boto3.resource('s3')
        obj = self.object_path.split('\\').pop()
        try:
            self.s3.Object(self.bucket, obj).delete()
            print('Object deleted')
        except:
            print('Failed to delete file') 
            
            
    def upload_object(self):
        """ Upload object to S3 """
        
        self.s3 = boto3.client('s3')
        obj = self.object_path.split('\\').pop()
        try:
            with open(self.object_path, "rb") as f:
                self.s3.upload_fileobj(f, self.bucket, obj)
            print('Object uploaded.')            
        except:
            print('Object not uploaded')


file = aws_s3('AK***************XR7', 'F************ooWsS/8mChM*********', 'bucket name' , r'path/file.csv')
file.s3_connection()
file.delete_object()
file.upload_object()

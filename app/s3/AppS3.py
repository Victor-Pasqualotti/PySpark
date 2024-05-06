from s3.AppSchemaS3 import AppSchemaS3

class AppS3(AppSchemaS3):
    def __init__(self, bucket:str = None):
        super().__init__(bucket = bucket)

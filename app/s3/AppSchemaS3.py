import boto3
import json

class AppSchemaS3:
    s3_resource = boto3.resource('s3')
    s3_client   = boto3.client('s3')
    arquivo_app_schema = "app_schema.json"

    def __init__(self, bucket:str = None):
        self.__nome_bucket = bucket
        self.__bucket      = boto3.resource('s3').Bucket(bucket)
        
        with open(f'{self.arquivo_app_schema}') as schema:
            self.__app_schema = json.load(schema)

    def valida_e_cria_schema(self):
        resp_bool, keys = self.se_bucket_tem_schema()
        if not resp_bool:
            schema_faltante = self.resgata_relacao_schema_atual(keys)["inexistentes"]
            self.cria_schema_app(schema_faltante)

    def se_bucket_tem_schema(self):
        s3_keys = self.resgata_valores_dict(self.__app_schema["paths"])
        if any(key for key in s3_keys if self.verifica_s3_key(key) is None):
            return False, s3_keys
        else:
            return True, s3_keys
            
    def resgata_valores_dict(self, dicionario ):
        valores = []
        if isinstance(dicionario, dict):

            for value in dicionario.values():
                if isinstance(value, dict):
                    valores.extend(self.resgata_valores_dict(value))

                if isinstance(value, list):
                    for v in value:
                        valores.extend(self.resgata_valores_dict(v))
                
                if isinstance(value, str):
                    valores.append(value)

        if isinstance(value, str):
            valores.append(value)
                
        return valores

    def verifica_s3_key(self, key:str = None):
        for obj in self.__bucket.objects.filter(Prefix = self.__app_schema["name"]):
            if obj.key.startswith(f'{key}'):
                return obj.key 
            else:
                False

    def resgata_relacao_schema_atual(self, schema):
        resposta = {
            "inexistentes": [
                key for key in schema if self.verifica_s3_key(key) is None
            ],
            "existentes" :[
                key for key in schema if self.verifica_s3_key(key) is not None
            ]
        }
        return resposta

    def cria_schema_app(self, schema):
        for key in schema:
            self.s3_client.put_object(
                Bucket = self.__nome_bucket
                ,Key   = f'{key}temp.txt'
                ,Body  = '' 
            )

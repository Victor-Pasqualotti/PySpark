
class AppGlueJob:

    def __init__(self, spark = None, glueContext = None):
        self.spark       = spark
        self.glueContext = glueContext

    def resgata_predicado(self, database:str = None, tabela:str = None):
        self.spark("""SHOW PARTITIONS""")
    
import datetime as dt
import pyspark.sql.functions as F

class Predicado:

    def __init__(self, db=None, tb=None, predicado=None, spark=None, glue_context=None) -> None:
        self.db = db
        self.tb = tb
        self.predicado = predicado
        self.spark = spark
        self.glue_context = glue_context

    def resgata_predicado_origens(self, db, tb, predicado):
        _df_sp = self.resgata_particoes(db, tb)
        self.resgata_coluna_ignorar(_df_sp)
        _df_fmt= self.formata_df_particoes(_df_sp)
        _dicionario = self.regata_n_particoes(_df_fmt, predicado)
        _predicado = self.monta_predicado(_dicionario)
        return _predicado

    def resgata_particoes(self, db, tb):
        """
        Performa a consulta para resgatar partições.
        Retorna um DataFrame como:
        +--------------------+
        |           partition|
        +--------------------+
        |ano=2024/mes=6/di...|
        |ano=2024/mes=6/dia=9|
        |ano=2024/mes=6/dia=8|
        |ano=2024/mes=6/dia=7|
        |ano=2024/mes=6/dia=6|
        +--------------------+
        """
        _df = self.spark.sql(f"SHOW PARTITIONS {db}.{tb}")
        return _df
    
    def resgata_coluna_ignorar(self, df):
        """
        Para o exemplo abaixo, 
        configura um paramentro como o nome da coluna:
        "partition"
        +--------------------+
        |           partition|
        +--------------------+
        |ano=2024/mes=6/di...|
        |ano=2024/mes=6/dia=9|
        |ano=2024/mes=6/dia=8|
        |ano=2024/mes=6/dia=7|
        |ano=2024/mes=6/dia=6|
        +--------------------+
        """
        self.col_ptc = df.columns[0]

    def formata_df_particoes(self, df):
        """
        Faz a quebra do texto, separando 
        as partições em colunas.
        Retorna um DataFrame como:
        +--------------------+----+---+---+
        |           partition| ano|mes|dia|
        +--------------------+----+---+---+
        |ano=2024/mes=6/di...|2024|  6| 10|
        |ano=2024/mes=6/dia=9|2024|  6|  9|
        |ano=2024/mes=6/dia=8|2024|  6|  8|
        |ano=2024/mes=6/dia=7|2024|  6|  7|
        |ano=2024/mes=6/dia=6|2024|  6|  6|
        +--------------------+----+---+---+
        Também funciona para partições de uma unica
        coluna
        """
        cols    = df.first()[0].split('/')
        qnt     = len(cols)

        for i in range(qnt):
            df = df.withColumn(
                cols[i].split('=')[0]
                ,F.split(
                    F.split(
                        F.col(self.col_ptc)
                        ,'/'
                    ).getItem(i)
                    ,'='
                ).getItem(1)
            )
        return df

    def regata_n_particoes(self, df, predicado):
        """
        Performa um SQL sobre o dataframe contendo
        os dados sobre as partições disponíveis.
        Ex (partições = anomesdia=yyyyMMdd/categoria='U|D'):
        +--------------------+---------+---------+
        |           partition|anomesdia|categoria|
        +--------------------+---------+---------+
        |anomesdia=2024061...| 20240610|        U|
        |anomesdia=2024060...| 20240609|        D|
        |anomesdia=2024060...| 20240608|        U|
        |anomesdia=2024060...| 20240607|        U|
        |anomesdia=2024060...| 20240606|        U|
        +--------------------+---------+---------+ 
        
        Maximo valor anomesdia
        "anomesdia = (select max(cast(anomesdia as int)) from partitions where categoria = 'D')"
        Três ultimas particoes
        "anomesdia in (select anomesdia from partitions order by anomesdia desc limit 3)"

        E com o resultado, cria um dicionario
        com os valores de cada partição.
        Ex:
        {
        'anomesdia': ['20240610', '20240609','20240608'],
        'categoria': ['U', 'D', 'U']
        }

        """
        df.createOrReplaceTempView('partitions')
        _df = self.spark.sql(f"SELECT * FROM partitions WHERE {predicado}")
        
        dicionario = {col : [] for col in _df.columns if col != self.col_ptc}
        for row in _df.collect():
            for col in dicionario.keys():
                dicionario[col].append(row[col])
        return dicionario
    
    def monta_predicado(self, dicionario):
        """
        Dado um dicionario como:
        {
        'anomesdia': ['20240610', '20240609','20240608'],
        'categoria': ['U', 'D', 'U']
        }

        Retorna a expressão de predicado como:
        '( anomesdia = 20240610 and  categoria = U) or ( anomesdia = 20240609 and  categoria = D) or ( anomesdia = 20240608 and  categoria = U)'
        """
        lista_predicados = list()
        qnt = len( list( dicionario.values() )[0] )

        for i in range(qnt):
            txt = " and ".join(f" {ptc} = {dicionario[ptc][i]}" for ptc in dicionario.keys())
            txt_predicado = "(" + txt + ")"
            lista_predicados.append(txt_predicado)
        
        filtro_predicado = " or ".join(predicado for predicado in lista_predicados)
        return filtro_predicado
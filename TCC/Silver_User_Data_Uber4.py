# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# COMMAND ----------

df_uber = spark.sql("""
                    SELECT *
                    FROM tcc_bronze.data_uber_user4
                    """)

# COMMAND ----------

df_uber = (df_uber.withColumnRenamed('city', 'Cidade')
                  .withColumnRenamed('product_type', 'TipoProduto')
                  .withColumnRenamed('status', 'StatusCorrida')
                  .withColumnRenamed('request_time', 'DataSolicitacao')
                  .withColumnRenamed('begin_trip_time', 'DataInicioViagem')
                  .withColumnRenamed('begintrip_lat', 'LatitudeInicioViagem')
                  .withColumnRenamed('begintrip_lng', 'LongitudeInicioViagem')
                  .withColumnRenamed('begintrip_address', 'EnderecoInicioViagem')
                  .withColumnRenamed('dropoff_time', 'DataFimViagem')
                  .withColumnRenamed('dropoff_lat', 'LatitudeDestinoViagem')
                  .withColumnRenamed('dropoff_lng', 'LongitudeDestinoViagem')
                  .withColumnRenamed('dropoff_address', 'EnderecoDestinoViagem')
                  .withColumnRenamed('distance', 'DistanciaPercorridaEmMilhas')
                  .withColumnRenamed('fare_amount', 'ValorPago')
                  .withColumnRenamed('fare_currency', 'Tipo Produto')
)

# COMMAND ----------

df_uber = (df_uber.withColumn("DataSolicitacao", expr("substr(DataSolicitacao,1,19)"))
                  .withColumn("DataInicioViagem", expr("substr(DataInicioViagem,1,19)"))
                  .withColumn("DataFimViagem", expr("substr(DataFimViagem,1,19)"))
           )


# COMMAND ----------

df_uber = (df_uber.withColumn("DataSolicitacao", col("DataSolicitacao").cast("timestamp")) 
.withColumn("DataInicioViagem", col("DataInicioViagem").cast("timestamp")) 
.withColumn("DataFimViagem", col("DataFimViagem").cast("timestamp"))
.withColumn("DistanciaPercorridaEmMilhas", col("DistanciaPercorridaEmMilhas").cast("float")) 
.withColumn("ValorPago", col("ValorPago").cast("float"))
)

# COMMAND ----------

df_uber = (df_uber.withColumn("AnoViagem", year(df_uber.DataSolicitacao))
.withColumn("MesVaigem", month(df_uber.DataSolicitacao))
.withColumn("DiaViagem", dayofyear(df_uber.DataSolicitacao))
.withColumn("DiaPorExtenso", date_format(df_uber.DataSolicitacao, "EEEE"))
           )

# COMMAND ----------

df_uber.printSchema()

# COMMAND ----------

df_uber = df_uber.withColumn("TempoDeEsperaViagem", col('DataInicioViagem').cast("long") - col('DataSolicitacao').cast("long")) 



# COMMAND ----------

# DBTITLE 1,r
df_uber = df_uber.withColumn("TempoDeEsperaViagem", col('TempoDeEsperaViagem') /60)
df_uber = df_uber.withColumn("TempoDeEsperaViagem", format_number(col("TempoDeEsperaViagem"), 2))
display(df_uber)

# COMMAND ----------

df_uber = (df_uber.withColumn("DiaDaSemana", when(col("DiaPorExtenso")=="Sunday", "Domingo")
.when(col("DiaPorExtenso")=="Monday","Segunda")
.when(col("DiaPorExtenso")=="Tuesday","Terça") 
.when(col("DiaPorExtenso")=="Wednesday","Quarta") 
.when(col("DiaPorExtenso")=="Thursday","Quinta")
.when(col("DiaPorExtenso")=="Friday","Sexta") 
.when(col("DiaPorExtenso")=="Saturday","Sabado")
.when(col("DiaPorExtenso")=="Sunday","Sabado")))

# COMMAND ----------

display(df_uber)

# COMMAND ----------

df_uber = (df_uber.withColumn("TipoProduto", when((col("TipoProduto")=="uberX") | (col("TipoProduto")=="UberX") | (col("TipoProduto")=="uberx") ,"Uber X")
.when((col("TipoProduto")=="Comfort Planet") | (col("TipoProduto")=="Comfort") , "Uber Comfort")
.when((col("TipoProduto")=="SELECT") | (col("TipoProduto")=="Select") | (col("TipoProduto")=="UberSELECT") , "Uber Select")
.when(col("TipoProduto")=="Moto","Uber Moto") 
.when(col("TipoProduto")=="Flash Moto","Flash Moto") 
.when(col("TipoProduto")=="VIP","Uber Vip") 
.when(col("TipoProduto")=="Flash","Uber Flash")
.when((col("TipoProduto")=="Black") | (col("TipoProduto")=="UberBlack") | (col("TipoProduto")=="UberBLACK") ,"Uber Black")
.when(col("TipoProduto")=="Prioridade","Uber Prioridade")
.when(col("TipoProduto")=="Uber Promo","Uber Promo")
.when(col("TipoProduto")=="UberVIP","Uber Vip")
.when(col("TipoProduto")=="null","Não informado")))

display(df_uber)

# COMMAND ----------

df_uber = df_uber.withColumn("DistanciaPercorridaEmKM", col("DistanciaPercorridaEmMilhas") * 1.60934) 
df_uber = df_uber.drop('DistanciaPercorridaEmMilhas')
df_uber = df_uber.drop('DiaPorExtenso')

# COMMAND ----------

df_uber = (df_uber.withColumn("StatusCorrida", when(col("StatusCorrida")=="completed","Concluida")
.when(col("StatusCorrida")=="unfulfilled","Descartada")
.when(col("StatusCorrida")=="rider_canceled","Cancelada") 
.when(col("StatusCorrida")=="driver_canceled","Motorista Cancelou") 
.when(col("StatusCorrida")=="fare_split","Corrida Dividida")))
display(df_uber)

# COMMAND ----------

# Tratamento dos nulos

# Data Inicio e Data Fim
df_uber = df_uber.withColumn("DataSolicitacao",when(col("DataSolicitacao").isNull(), "9999-12-31 00:00:00").otherwise(col("DataSolicitacao")))

# Data Inicio e Data Fim
df_uber = df_uber.withColumn("DataInicioViagem",when(col("DataInicioViagem").isNull(), "9999-12-31 00:00:00").otherwise(col("DataInicioViagem")))
df_uber = df_uber.withColumn("DataFimViagem",when(col("DataFimViagem").isNull(), "9999-12-31 00:00:00").otherwise(col("DataFimViagem")))

# LAT e LONG Inicio
df_uber = (df_uber.withColumn("LatitudeInicioViagem", when(col("LatitudeInicioViagem").isNull(),"-1").otherwise(col("LatitudeInicioViagem"))))
df_uber = (df_uber.withColumn("LongitudeInicioViagem", when(col("LongitudeInicioViagem").isNull(),"-1").otherwise(col("LongitudeInicioViagem"))))

# LAT e LONG Fim
df_uber = (df_uber.withColumn("LatitudeDestinoViagem", when(col("LatitudeDestinoViagem").isNull(),"-1").otherwise(col("LatitudeDestinoViagem"))))
df_uber = (df_uber.withColumn("LongitudeDestinoViagem", when(col("LongitudeDestinoViagem").isNull(),"-1").otherwise(col("LongitudeDestinoViagem"))))

# Endereços da Vaigem
df_uber = (df_uber.withColumn("EnderecoInicioViagem", when(col("EnderecoInicioViagem").isNull(),"Endereço não Informado").otherwise(col("EnderecoInicioViagem"))))
df_uber = (df_uber.withColumn("EnderecoDestinoViagem", when(col("EnderecoDestinoViagem").isNull(),"Endereço não Informado").otherwise(col("EnderecoDestinoViagem"))))

# Tipo do Produto
df_uber = (df_uber.withColumn("Tipo Produto", when(col("Tipo Produto").isNull(),"Brazilian Real").otherwise(col("Tipo Produto"))))

# Valor Pago
df_uber = (df_uber.withColumn("ValorPago", when(col("ValorPago").isNull(),"-1").otherwise(col("ValorPago"))))

# TempoDeEsperaViagem
df_uber = (df_uber.withColumn("TempoDeEsperaViagem", when(col("TempoDeEsperaViagem").isNull(),"-1").otherwise(col("TempoDeEsperaViagem"))))

display(df_uber)

# COMMAND ----------

df_uber = df_uber.select(
'Cidade',
'TipoProduto',
'StatusCorrida',
'DataSolicitacao',
'TempoDeEsperaViagem',
'ValorPago',
'DiaViagem',
'MesVaigem',
'AnoViagem',
'DiaDaSemana',
'DistanciaPercorridaEmKM'
)

# COMMAND ----------

display(df_uber)

# COMMAND ----------

table = 'data_uber_user4'
database_name = 'tcc_silver'

df_uber.write.format("delta").saveAsTable(f"{database_name}.{table}")

# -- coding: utf-8 --
import datetime
import sys
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
import argparse
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *
from create import *


timestart = datetime.now() 
parser = argparse.ArgumentParser()
parser.add_argument('--vMes', required=True, type=int)
parser.add_argument('--vUltimo_dia_mes', required=True, type=str)
parser.add_argument('--vTabla_tuenti_account', required=True, type=str)
parser.add_argument('--vTabla_tuenti_msisdn', required=True, type=str)
parser.add_argument('--vParticion', required=True, type=str)
parser.add_argument('--bd', required=True, type=str)
parametros = parser.parse_args()
vMes = parametros.vMes
vUltimo_dia_mes = parametros.vUltimo_dia_mes
vTabla_tuenti_account = parametros.vTabla_tuenti_account
vTabla_tuenti_msisdn = parametros.vTabla_tuenti_msisdn
vParticion = parametros.vParticion
bd = parametros.bd

spark = SparkSession\
    .builder\
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

vSql = """
SELECT b.create_date AS fecha_alta,
CONCAT(a.name,' ',a.last_name) AS nombre,
CONCAT('593',SUBSTRING(msisdn,2,10)) AS msisdn,
UPPER(MD5(CONCAT(CONCAT('593',SUBSTRING(msisdn,2,10)),'{vMes}'))) AS id_hash,
'{vUltimo_dia_mes}' AS fecha_proceso,
{vMes} AS pt_mes
FROM {vTabla_tuenti_account} a
INNER JOIN {vTabla_tuenti_msisdn} b
ON a.id_account=b.id_account
WHERE 
b.pt_mes={vMes} 
"""

print(etq_sql(vSql))
timestart_tbl = datetime.now()
df0 = spark.sql(vSql.format(vMes=vMes,vUltimo_dia_mes=vUltimo_dia_mes,vTabla_tuenti_account=vTabla_tuenti_account,vTabla_tuenti_msisdn=vTabla_tuenti_msisdn))
vTotDf=df0.count()
print("Total registros: "+ str(vTotDf))
print ("==== Guardando los datos en tabla "+bd+" ====")
query_truncate = "ALTER TABLE "+bd+" DROP IF EXISTS PARTITION ("+vParticion+" = "+str(vMes)+") purge"
hc=HiveContext(spark)
hc.sql(query_truncate)
df0.repartition(1).write.mode("append").insertInto(bd)
df0.printSchema()
timeend_tbl = datetime.now()
duracion_tbl = timeend_tbl - timestart_tbl
print("Escritura Exitosa de la tabla "+bd)
print("Duracion create "+bd+" {}".format(duracion_tbl))

spark.stop()
timeend = datetime.now()
duracion = timeend - timestart
print("Duracion: {vDuracion}".format(vDuracion=duracion))

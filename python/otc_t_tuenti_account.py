# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import datetime
import argparse
from pyspark.sql import SparkSession
import time
from pyspark.sql import functions as F
from pyspark.sql import HiveContext
import base64

desde = time.time()

## Captura de parametros de entrada
parser = argparse.ArgumentParser()
parser.add_argument('--vFecha_Inicial', required=True, type=str)
parser.add_argument('--vFecha_Final', required=True, type=str)
parser.add_argument('--vParticion', required=True, type=str)
parser.add_argument('--bd', required=True, type=str)
parser.add_argument('--vPt_mes', required=True, type=int)
parametros = parser.parse_args()
vFecha_Inicial = parametros.vFecha_Inicial
vFecha_Final = parametros.vFecha_Final
vParticion = parametros.vParticion
bd = parametros.bd
vPt_mes = parametros.vPt_mes

spark = SparkSession\
    .builder\
    .appName("OTC_T_TUENTI_ACCOUNT")\
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.yarn.queue", "reportes") \
    .config("hive.enforce.bucketing", "false")\
    .config("hive.enforce.sorting", "false")\
    .master("local")\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

vSQL="""
(
SELECT id_account,
name,
last_name,
email,
password,
terms_accepted,
social_id,
platform,
token,
create_date,
source,
app_version,
platform_version,
browser,
browser_version,
media_source,
last_login,
imei,
device_model,
device_manufacturer,
nickname,
gender,
birthday,
{0}
FROM otc_tuenti.otc_t_tuenti_account 
WHERE to_char(create_date,'YYYYMMDD')::int>={1}
AND to_char(create_date,'YYYYMMDD')::int<{2} 
) AS otc_t_tuenti_account
"""

vHost="10.112.152.230"
vPort="5432"
vDataBase="otc_tuenti"
vUrl="jdbc:postgresql://{0}:{1}/{2}".format(vHost,vPort,vDataBase)
vClass="org.postgresql.Driver"
vUsuario="NABPTNTI001"
vClave=base64.b64decode(spark.conf.get("spark.jdbc.tuenti.bpassword"))
vDriver="org.postgresql.Driver"

df0 = spark.\
    read.\
    format("jdbc").\
    option("url", vUrl).\
    option("driver", vDriver).\
    option("dbtable", vSQL.format(vPt_mes,vFecha_Inicial,vFecha_Final)).\
    option("user", vUsuario).\
    option("password", vClave).\
    load()

df0.printSchema()
#df0.show(10)

timestart_tbl = datetime.datetime.now()
print ("==== Guardando los datos en tabla "+bd+" ====")
query_truncate = "ALTER TABLE "+bd+" DROP IF EXISTS PARTITION ("+vParticion+" = "+str(vPt_mes)+") purge"
hc=HiveContext(spark)
hc.sql(query_truncate)
df0.repartition(1).write.mode("append").insertInto(bd)
df0.printSchema()
timeend_tbl = datetime.datetime.now()
duracion_tbl = timeend_tbl - timestart_tbl
print("Escritura Exitosa de la tabla "+bd+" particion "+str(vParticion))
print("Duracion create "+bd+" {}".format(duracion_tbl))

spark.stop()
hasta = time.time()
duracion = hasta - desde
print("Duracion: {vDuracion}".format(vDuracion=duracion))

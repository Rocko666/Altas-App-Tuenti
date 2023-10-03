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
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *

desde = time.time()

## Captura de parametros de entrada
parser = argparse.ArgumentParser()
parser.add_argument('--vFecha_Inicial', required=True, type=str)
parser.add_argument('--vFecha_Final', required=True, type=str)
parser.add_argument('--vParticion', required=True, type=str)
parser.add_argument('--bd', required=True, type=str)
parser.add_argument('--vPt_mes', required=True, type=int)
parser.add_argument('--vHost', required=True, type=str)
parser.add_argument('--vPort', required=True, type=str)
parser.add_argument('--vDataBase', required=True, type=str)
parser.add_argument('--vDriver', required=True, type=str)
parser.add_argument('--vUsuario', required=True, type=str)
parametros = parser.parse_args()
vFecha_Inicial = parametros.vFecha_Inicial
vFecha_Final = parametros.vFecha_Final
vParticion = parametros.vParticion
bd = parametros.bd
vPt_mes = parametros.vPt_mes
vHost=parametros.vHost
vPort=parametros.vPort
vDataBase=parametros.vDataBase
vDriver=parametros.vDriver
vUsuario=parametros.vUsuario

spark = SparkSession\
    .builder\
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
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
) AS otc_t_tuenti_account
"""

vUrl="jdbc:postgresql://{0}:{1}/{2}".format(vHost,vPort,vDataBase)
vClave=base64.b64decode(spark.conf.get("spark.jdbc.tuenti.bpassword"))

df0 = spark.\
    read.\
    format("jdbc").\
    option("url", vUrl).\
    option("driver", vDriver).\
    option("dbtable", vSQL.format(vPt_mes)).\
    option("user", vUsuario).\
    option("password", vClave).\
    load()

df0.printSchema()

timestart_tbl = datetime.datetime.now()
print ("==== Guardando los datos en tabla "+bd+" ====")
query_truncate = "ALTER TABLE "+bd+" DROP IF EXISTS PARTITION ("+vParticion+" = "+str(vPt_mes)+") purge"
hc=HiveContext(spark)
hc.sql(query_truncate)
df0.repartition(1).write.mode("append").insertInto(bd)
timeend_tbl = datetime.datetime.now()
duracion_tbl = timeend_tbl - timestart_tbl
print("Escritura Exitosa de la tabla "+bd+" particion "+str(vParticion))
print("Duracion create "+bd+" {}".format(duracion_tbl))

spark.stop()
hasta = time.time()
duracion = hasta - desde
print("Duracion: {vDuracion}".format(vDuracion=duracion))

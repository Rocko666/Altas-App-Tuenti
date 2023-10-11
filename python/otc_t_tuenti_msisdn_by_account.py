# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from datetime import datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import base64
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *

## Captura de parametros de entrada
parser = argparse.ArgumentParser()
parser.add_argument('--vFecha_Inicial', required=True, type=str)
parser.add_argument('--vFecha_Final', required=True, type=str)
parser.add_argument('--vParticion', required=True, type=str)
parser.add_argument('--vMainTable', required=True, type=str)
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
vMainTable = parametros.vMainTable
vPt_mes = parametros.vPt_mes
vHost=parametros.vHost
vPort=parametros.vPort
vDataBase=parametros.vDataBase
vDriver=parametros.vDriver
vUsuario=parametros.vUsuario

## Query a ejecutarse en POSTGRESS 
vSQL="""
(SELECT 
    id_msisdn_by_account,
    id_account,
    msisdn,
    notification_push,
    create_date,
    agent,
    agent_id,
    alias,
    status,
    token_push,
    {0}
FROM otc_tuenti.otc_t_tuenti_msisdn_by_account 
WHERE to_char(create_date,'YYYYMMDD')::int>={1}
AND to_char(create_date,'YYYYMMDD')::int<{2} ) 
AS otc_t_tuenti_msisdn_by_account
"""

## 2.- Inicio del SparkSession
spark = SparkSession\
    .builder\
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId

vUrl="jdbc:postgresql://{0}:{1}/{2}".format(vHost,vPort,vDataBase)
vClave=base64.b64decode(spark.conf.get("spark.jdbc.tuenti.bpassword"))

timestart = datetime.now()
print(lne_dvs())
vStp00='Paso [0]: Iniciando proceso/Cargando configuracion..'
try:
    ts_step = datetime.now()
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vFecha_Inicial",str(vFecha_Inicial))))
    print(etq_info(log_p_parametros("vFecha_Final",str(vFecha_Final))))
    print(etq_info(log_p_parametros("vParticion",str(vParticion))))
    print(etq_info(log_p_parametros("vMainTable",str(vMainTable))))
    print(etq_info(log_p_parametros("vPt_mes",str(vPt_mes))))
    print(etq_info(log_p_parametros("vHost",str(vHost))))
    print(etq_info(log_p_parametros("vPort",str(vPort))))
    print(etq_info(log_p_parametros("vDataBase",str(vDataBase))))
    print(etq_info(log_p_parametros("vDriver",str(vDriver))))
    print(etq_info(log_p_parametros("vUsuario",str(vUsuario))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp00,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp00,str(e))))

print(lne_dvs())
vStp01='PASO [1]: Conexion a la base de datos y escritura: '
try:
    ts_step = datetime.now()  
    print(etq_info(str(vStp01)))
    print(etq_sql(vSQL))
    df0 = spark.\
        read.\
        format("jdbc").\
        option("url", vUrl).\
        option("driver", vDriver).\
        option("dbtable", vSQL.format(vPt_mes,vFecha_Inicial,vFecha_Final)).\
        option("user", vUsuario).\
        option("password", vClave).\
        load()
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        df1 = df0
        df1 = df1.select([F.col(x).alias(x.lower()) for x in df1.columns])
        df1.printSchema()
    te_step = datetime.now()
    print(etq_info(('Total de registros a insertarse en tabla ',vMainTable,':',str(df1.count()))))
    print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp01,str(e))))
print(lne_dvs())

print("***************************TABLA A CARGAR****************************")
vStp02='PASO [2]: Borrado e insercion de datos Hive'
try:
    print(etq_info(str(vStp02)))
    print(lne_dvs())
    print(etq_info("REALIZA EL BORRADO DE PARTICIONES CORRESPONDIENTES AL MES DE PROCESO"))
    query_truncate = "ALTER TABLE "+vMainTable+" DROP IF EXISTS PARTITION ("+vParticion+" = "+str(vPt_mes)+") purge"
    print(query_truncate)
    spark.sql(query_truncate)
    print(etq_info(msg_i_insert_hive(vMainTable)))
    df1.repartition(1).write.mode("append").insertInto(vMainTable)
    print(etq_info(('Total de registros insertados en tabla principal ',vMainTable,':',str(df1.count()))))
    print(lne_dvs())
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_insert_hive(vMainTable,str(e))))  

## 4.- Cierre
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion('otc_t_tuenti_msisdn_by_account.py',vle_duracion(timestart,timeend))))
print(lne_dvs())


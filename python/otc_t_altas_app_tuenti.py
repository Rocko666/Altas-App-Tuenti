# -- coding: utf-8 --
import datetime
import sys
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import argparse
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

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
app_id = spark._sc.applicationId

vSql = """
SELECT 
    b.create_date AS fecha_alta,
    CONCAT(a.name,' ',a.last_name) AS nombre,
    CONCAT('593',SUBSTRING(msisdn,2,10)) AS msisdn,
    UPPER(MD5(CONCAT(CONCAT('593',SUBSTRING(msisdn,2,10)),'{vMes}'))) AS id_hash,
    '{vUltimo_dia_mes}' AS fecha_proceso,
    {vMes} AS pt_mes
FROM {vTabla_tuenti_account} a
INNER JOIN {vTabla_tuenti_msisdn} b
    ON a.id_account=b.id_account
WHERE b.pt_mes={vMes} 
    AND a.pt_mes={vMes} 
"""

timestart = datetime.now()
print(lne_dvs())
vStp00='Paso [1]: Iniciando proceso/Cargando configuracion..'
try:
    ts_step = datetime.now()
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vMes",str(vMes))))
    print(etq_info(log_p_parametros("vUltimo_dia_mes",str(vUltimo_dia_mes))))
    print(etq_info(log_p_parametros("vTabla_tuenti_account",str(vTabla_tuenti_account))))
    print(etq_info(log_p_parametros("vTabla_tuenti_msisdn",str(vTabla_tuenti_msisdn))))
    print(etq_info(log_p_parametros("vParticion",str(vParticion))))
    print(etq_info(log_p_parametros("bd",str(bd))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp00,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp00,str(e))))

print("***************************TABLA A CARGAR****************************")
vStp02='PASO [2]: Borrado e insercion de datos Hive'
try:
    print(etq_info(str(vStp02)))
    print(lne_dvs())
    print(etq_info("SE EJECUTA EL SIGUIENTE QUERY: "))
    print(etq_sql(vSql))
    df0 = spark.sql(vSql.format(vMes=vMes,vUltimo_dia_mes=vUltimo_dia_mes,vTabla_tuenti_account=vTabla_tuenti_account,vTabla_tuenti_msisdn=vTabla_tuenti_msisdn))
    df0.printSchema()
    print(etq_info("REALIZA EL BORRADO DE PARTICIONES CORRESPONDIENTES AL MES DE PROCESO"))
    query_truncate = "ALTER TABLE "+bd+" DROP IF EXISTS PARTITION ("+vParticion+" = "+str(vMes)+") purge"
    print(query_truncate)
    spark.sql(query_truncate)
    print(etq_info(msg_i_insert_hive(bd)))
    df0.repartition(1).write.mode("append").insertInto(bd)
    print(etq_info(('Total de registros insertados en tabla principal ',bd,':',str(df0.count()))))
    print(lne_dvs())
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_insert_hive(bd,str(e))))  

## 4.- Cierre
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion('otc_t_altas_app_tuenti.py',vle_duracion(timestart,timeend))))
print(lne_dvs())

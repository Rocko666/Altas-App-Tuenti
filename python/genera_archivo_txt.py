import sys
reload(sys)
sys.setdefaultencoding('UTF-8')
from pyspark.sql import SparkSession
import datetime
from pyspark.sql import functions as F, Window
import argparse
from pyspark.sql.functions import *

parser = argparse.ArgumentParser()
parser.add_argument('--vRuta', required=True, type=str)
parser.add_argument('--vTabla', required=True, type=str)
parser.add_argument('--vMes', required=True, type=str)
parametros = parser.parse_args()
vRuta=parametros.vRuta
vTabla=parametros.vTabla
vMes=parametros.vMes

timestartmain = datetime.datetime.now() 

spark = SparkSession\
	.builder\
	.appName("ALTAS_APP_TUENTI")\
	.enableHiveSupport()\
	.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

vSql = """
SELECT (CASE WHEN fecha_proceso IS NULL THEN '01/01/1990' ELSE 
concat_ws('/',SUBSTR(fecha_proceso,1,2),SUBSTR(fecha_proceso,4,2),SUBSTR(fecha_proceso,7,4)) END) AS fecha_proceso,
(CASE WHEN fecha_alta IS NULL THEN '01/01/1990' ELSE concat_ws('/',SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),9,2),
SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),6,2),SUBSTR(date_format(fecha_alta,'yyyy-MM-dd'),1,4)) END) AS fecha_alta_app,
msisdn,
id_hash
FROM {vTabla} 
WHERE pt_mes={vMes}
"""

timestart = datetime.datetime.now()
print ("==== Generando archivo "+vRuta+" ====")
df_final=spark.sql(vSql.format(vTabla=vTabla,vMes=vMes))
pandas_df = df_final.toPandas()
pandas_df.rename(columns = lambda x:x.upper(), inplace=True )
pandas_df.to_csv(vRuta, sep='|',index=False)
timeend = datetime.datetime.now()
duracion = timeend - timestart
print("Generacion exitosa del archivo "+vRuta)
print("Duracion genera archivo "+vRuta+" {}".format(duracion))

spark.stop()
timeendmain = datetime.datetime.now()
duracion = timestartmain- timeendmain 
print("Duracion: {vDuracion}".format(vDuracion=duracion))

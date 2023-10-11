set -e
#!/bin/bash
#########################################################################################################
# NOMBRE: OTC_T_TUENTI_ACCOUNT.sh 	    		     	      								            #
# DESCRIPCION:																							#
#   Shell que extrae la informacion de la tabla otc_t_tuenti_account de Postgress a Hive				#
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2022-08-31   																			#
# PARAMETROS DEL SHELL                            													    #
# VAL_FECHA_EJEC=${1} 		Fecha de ejecucion de proceso en formato  YYYYMMDD                          #
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
#  2023-06-19   Cristian Ortiz  Control errores, estandares cloudera                                    #
#  2023-10-05   Cristian Ortiz  Cambio de alcance equipo COMISIONES                                     #
#########################################################################################################
##############
# VARIABLES #
##############
ENTIDAD=URMTNTACCNT0010

#PARAMETROS QUE RECIBE LA SHELL
VAL_FECHA_EJEC=$1
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
VAL_ESQUEMA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA';"`
VAL_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA';"`
VAL_CAMPO_PARTICION=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CAMPO_PARTICION';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`
VAL_QUEUE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_QUEUE';"`
VAL_HOST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HOST';"`
VAL_PORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PORT';"`
VAL_DB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DB';"`
VAL_DRIVER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER';"`
VAL_USUARIO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_USUARIO';"`
VAL_JAR=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_JAR';"`
VAL_PROP_FILE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PROP_FILE';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FEC_AYER=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y%m%d"`
VAL_FEC_FIN=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m01"`
VAL_FEC_INI=`date -d "${VAL_FEC_FIN} -1 day"  +"%Y%m01"`
VAL_PT_MES=`date -d "${VAL_FEC_INI}"  +"%Y%m"`
VAL_BD=$VAL_ESQUEMA.$VAL_TABLA
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/OTC_T_TUENTI_ACCOUNT_$VAL_DIA$VAL_HORA.log

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros del SPARK GENERICO" 
###########################################################################################################################################################
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_FECHA_EJEC" ] ||  
    [ -z "$VAL_RUTA" ] || 
    [ -z "$VAL_ESQUEMA" ] || 
    [ -z "$VAL_TABLA" ] || 
    [ -z "$VAL_CAMPO_PARTICION" ] || 
    [ -z "$VAL_MASTER" ] || 
    [ -z "$VAL_DRIVER_MEMORY" ] || 
    [ -z "$VAL_EXECUTOR_MEMORY" ] || 
    [ -z "$VAL_NUM_EXECUTORS" ] || 
    [ -z "$VAL_NUM_EXECUTORS_CORES" ] || 
    [ -z "$VAL_QUEUE" ] || 
    [ -z "$VAL_HOST" ] || 
    [ -z "$VAL_PORT" ] || 
    [ -z "$VAL_DRIVER" ] || 
    [ -z "$VAL_USUARIO" ] || 
    [ -z "$VAL_JAR" ] || 
    [ -z "$VAL_PROP_FILE" ] || 
    [ -z "$VAL_BD" ] || 
    [ -z "$VAL_RUTA_SPARK" ] || 
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia extraccion tabla OTC_T_TUENTI_ACCOUNT de Postgress ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" 2>&1 &>> $VAL_LOG
echo "Fecha Particion: $VAL_PT_MES" 2>&1 &>> $VAL_LOG

#REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
echo "==== Ejecuta archivo spark otc_t_tuenti_account.py que extrae informacion de Postgress a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--properties-file $VAL_PROP_FILE \
--jars $VAL_JAR \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
$VAL_RUTA/python/otc_t_tuenti_account.py \
--vParticion=$VAL_CAMPO_PARTICION \
--vMainTable=$VAL_BD \
--vHost=$VAL_HOST \
--vPort=$VAL_PORT \
--vDataBase=$VAL_DB \
--vDriver=$VAL_DRIVER \
--vUsuario=$VAL_USUARIO \
--vPt_mes=$VAL_PT_MES 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark otc_t_tuenti_account.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
	echo "==== ERROR: - En la ejecucion del archivo spark otc_t_tuenti_account.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

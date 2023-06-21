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

#PARAMETROS GENERICOS
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_USUARIO=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`

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
vRUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_FECHA_EJEC" ] || 
    [ -z "$VAL_COLA_EJECUCION" ] || 
    [ -z "$VAL_CADENA_JDBC" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$VAL_USUARIO" ] || 
    [ -z "$VAL_ESQUEMA" ] || 
    [ -z "$VAL_TABLA" ] || 
    [ -z "$VAL_CAMPO_PARTICION" ] || 
    [ -z "$VAL_BD" ] || 
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia extraccion tabla OTC_T_TUENTI_ACCOUNT de Postgress ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" 2>&1 &>> $VAL_LOG
echo "Fecha Inicio: $VAL_FEC_INI" 2>&1 &>> $VAL_LOG
echo "Fecha Fin: $VAL_FEC_FIN" 2>&1 &>> $VAL_LOG
echo "Fecha Particion: $VAL_PT_MES" 2>&1 &>> $VAL_LOG

#REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
echo "==== Ejecuta archivo spark otc_t_tuenti_account.py que extrae informacion de Postgress a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
$vRUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.port.maxRetries=100 \
--properties-file /var/opt/tel_lib/credentials.properties \
--jars /var/opt/tel_lib/postgresql-42.2.2.jar \
--master local \
--executor-memory 16G \
--num-executors 4 \
--executor-cores 4 \
--driver-memory 16G \
$VAL_RUTA/python/otc_t_tuenti_account.py \
--vFecha_Inicial=$VAL_FEC_INI \
--vFecha_Final=$VAL_FEC_FIN \
--vParticion=$VAL_CAMPO_PARTICION \
--bd=$VAL_BD \
--vPt_mes=$VAL_PT_MES 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark otc_t_tuenti_account.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
	echo "==== ERROR: - En la ejecucion del archivo spark otc_t_tuenti_account.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

#VALIDA EXTRACCION DE REGISTROS
echo "==== Verifica que se hayan extraido registros en Hive ====" 2>&1 &>> $VAL_LOG
cant_reg_d=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT COUNT(1) FROM $VAL_ESQUEMA.$VAL_TABLA WHERE pt_mes=$VAL_PT_MES;")
echo "Cantidad registros destino: $cant_reg_d" 2>&1 &>> $VAL_LOG
	if [ $cant_reg_d -gt 0 ]; then
			echo "==== OK - Se extrajeron datos de la fuente ====" 2>&1 &>> $VAL_LOG
		else
			echo "==== ERROR: - No se extrajeron datos de la fuente ====" 2>&1 &>> $VAL_LOG
			exit 1
	fi

echo "==== Finaliza extraccion tabla OTC_T_TUENTI_ACCOUNT de Postgress ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	

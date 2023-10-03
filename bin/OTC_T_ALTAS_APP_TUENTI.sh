set -e
#!/bin/bash
#########################################################################################################
# NOMBRE: OTC_T_ALTAS_APP_TUENTI.sh 			    	      								            #
# DESCRIPCION:																							#
#   Shell principal del proceso de ALTAS APP TUENTI que hace el llamado al spark que realiza el proceso #
#   de cruc, genera la información en la tabla destino final otc_t_altas_app_tuenti en Hive				#
#   y genera archivo TXT en servidor FTP.
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2022-08-31   																			#
# PARAMETROS DEL SHELL                            													    #
# VAL_FECHA_EJEC=${1} 		Fecha de ejecucion de proceso en formato  YYYYMMDD                          #
# VAL_RUTA=${2} 			Ruta donde se encuentran los objetos del proceso                            #
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
# 2023-06-19   Cristian Ortiz  Control errores, estandares cloudera                                     #
#########################################################################################################
##############
# VARIABLES #
##############
ENTIDAD=D_EXTRALTSAPPTUENTI0020

# sh -x /home/nae105215/RAW/TUENTI/OTC_T_TUENTI_MSISDN_BY_ACCOUNT/bin/OTC_T_TUENTI_MSISDN_BY_ACCOUNT.sh 20230904 && sh -x /home/nae105215/RAW/TUENTI/OTC_T_TUENTI_ACCOUNT/bin/OTC_T_TUENTI_ACCOUNT.sh 20230904 && sh -x /home/nae105215/ALTAS_APP_TUENTI/bin/OTC_T_ALTAS_APP_TUENTI.sh 20230904 

#PARAMETROS QUE RECIBE LA SHELL
VAL_FECHA_EJEC=$1
VAL_RUTA=$2

#PARAMETROS DEFINIDOS EN LA TABLA params_des
ETAPA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_SFTP_PUERTO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_PUERTO';"`
VAL_SFTP_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_USER';"`
VAL_SFTP_HOSTNAME=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_HOSTNAME';"`
VAL_SFTP_PASS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_PASS';"`
VAL_SFTP_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_RUTA';"`
VAL_NOM_ARCHIVO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_ARCHIVO';"`
VAL_ESQUEMA_RAW=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_RAW';"`
VAL_ESQUEMA_HIVE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_HIVE';"`
VAL_TABLA_RAW_ACCOUNT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_RAW_ACCOUNT';"`
VAL_TABLA_RAW_MSISDN=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_RAW_MSISDN';"`
VAL_TABLA_TUENTI=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_TUENTI';"`
VAL_CAMPO_PT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CAMPO_PT';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`
VAL_QUEUE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_QUEUE';"`

#PARAMETROS GENERICOS
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FEC_AYER=`date -d "${VAL_FECHA_EJEC} -1 day"  +"%Y%m%d"`
VAL_FEC_FIN=`date -d "${VAL_FEC_AYER} -1 day"  +"%Y%m01"`
VAL_FEC_INI=`date -d "${VAL_FEC_FIN} -1 day"  +"%Y%m01"`
VAL_FIN_MES=`date -d "${VAL_FEC_FIN} -1 day"  +"%d-%m-%Y"`
VAL_PT_MES=`date -d "${VAL_FEC_INI}"  +"%Y%m"`
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/ALTAS_APP_TUENTI_$VAL_DIA$VAL_HORA.log
VAL_RUTA_ARCHIVO=$VAL_RUTA"/output/"$VAL_NOM_ARCHIVO

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros del SPARK GENERICO" 
###########################################################################################################################################################

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_FECHA_EJEC" ] || 
    [ -z "$ETAPA" ] || 
    [ -z "$VAL_SFTP_USER" ] || 
    [ -z "$VAL_SFTP_PUERTO" ] || 
    [ -z "$VAL_SFTP_HOSTNAME" ] || 
    [ -z "$VAL_SFTP_PASS" ] || 
    [ -z "$VAL_SFTP_RUTA" ] || 
    [ -z "$VAL_NOM_ARCHIVO" ] || 
    [ -z "$VAL_PT_MES" ] || 
    [ -z "$VAL_FIN_MES" ] || 
    [ -z "$VAL_LOG" ] || 
    [ -z "$VAL_RUTA_SPARK" ] || 
    [ -z "$VAL_ESQUEMA_RAW" ] || 
    [ -z "$VAL_ESQUEMA_HIVE" ] || 
    [ -z "$VAL_TABLA_RAW_ACCOUNT" ] || 
    [ -z "$VAL_TABLA_RAW_MSISDN" ] || 
    [ -z "$VAL_TABLA_TUENTI" ] || 
    [ -z "$VAL_CAMPO_PT" ] || 
    [ -z "$VAL_RUTA_ARCHIVO" ] || 
    [ -z "$VAL_RUTA" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion del proceso ALTAS APP TUENTI ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" 2>&1 &>> $VAL_LOG
echo "Fecha Particion: $VAL_PT_MES" 2>&1 &>> $VAL_LOG
echo "Fecha Proceso: $VAL_FIN_MES" 2>&1 &>> $VAL_LOG

#PASO 1: REALIZA EL LLAMADO AL ARCHIVO SPARK QUE EJECUTA LOGICA EN HIVE PARA GENERAR INFORMACION EN TABLA DESTINO
if [ "$ETAPA" = "1" ]; then
echo "==== Ejecuta archivo spark otc_t_altas_app_tuenti.py que carga informacion a Hive ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/otc_t_altas_app_tuenti.py \
--vMes=$VAL_PT_MES \
--vUltimo_dia_mes=$VAL_FIN_MES \
--vTabla_tuenti_account=$VAL_ESQUEMA_RAW"."$VAL_TABLA_RAW_ACCOUNT \
--vTabla_tuenti_msisdn=$VAL_ESQUEMA_RAW"."$VAL_TABLA_RAW_MSISDN \
--vParticion=$VAL_CAMPO_PT \
--bd=$VAL_ESQUEMA_HIVE"."$VAL_TABLA_TUENTI 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'AttributeError:|An error occurred|UnicodeDecodeError:|TypeError:|SyntaxError:|command not found|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark otc_t_altas_app_tuenti.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark otc_t_altas_app_tuenti.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi
ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params_des set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#PASO 2: LEE TABLA DE EXTRACTOR ALTAS APP TUENTI Y GENERA ARCHIVO TXT EN RUTA OUTPUT
if [ "$ETAPA" = "2" ]; then
rm -f ${VAL_RUTA}/output/*
echo "==== Lee tabla de Extractor ALTAS APP TUENTI y genera archivo en ruta output ====" 2>&1 &>> $VAL_LOG
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$VAL_RUTA/python/genera_archivo_txt.py \
--vTabla=$VAL_ESQUEMA_HIVE"."$VAL_TABLA_TUENTI \
--vRuta=$VAL_RUTA_ARCHIVO \
--vMes=$VAL_PT_MES 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Error PySpark:|error:|An error occurred|ERROR FileFormatWriter:|Traceback|SyntaxError|UnicodeDecodeError|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark genera_archivo_txt.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del archivo spark genera_archivo_txt.py ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

#VERIFICA SI EL ARCHIVO TXT CONTIENE DATOS
echo "==== Valida si el archivo TXT contiene datos ====" 2>&1 &>> $VAL_LOG
cant_reg=`wc -l ${VAL_RUTA_ARCHIVO}` 
echo $cant_reg 2>&1 &>> $VAL_LOG
cant_reg=`echo ${cant_reg}|cut -f1 -d" "` 
cant_reg=`expr $cant_reg + 0` 
	if [ $cant_reg -ne 0 ]; then
			echo "==== OK - El archivo TXT contiene datos para transferir al servidor FTP ====" 2>&1 &>> $VAL_LOG
		else
			echo "==== ERROR - El archivo TXT no contiene datos para transferir al servidor FTP ====" 2>&1 &>> $VAL_LOG
			exit 1
	fi
ETAPA=3
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params_des set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

vFTP_NOM_ARCHIVO_FORMATO='Extractor_Tuenti_APP_jul.txt'

#CREA FUNCION PARA LA EXPORTACION DEL ARCHIVO A RUTA FTP Y REALIZA LA TRANSFERENCIA
if [ "$ETAPA" = "3" ]; then
echo "==== Crea funcion para la exportacion del archivo a ruta FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
function exportar()
{
    /usr/bin/expect << EOF 2>&1 &>> $VAL_LOG
		set timeout -1
		spawn sftp ${VAL_SFTP_USER}@${VAL_SFTP_HOSTNAME} ${VAL_SFTP_PUERTO}
		expect "password:"
		send "${VAL_SFTP_PASS}\n"
		expect "sftp>"
		send "cd ${VAL_SFTP_RUTA}\n"
		expect "sftp>"
		send "put $VAL_RUTA_ARCHIVO $(basename ${vFTP_NOM_ARCHIVO_FORMATO})\n"
		expect "sftp>"
		send "exit\n"
		interact
EOF
}
# send "put $VAL_RUTA_ARCHIVO\n"
# send "put $VAL_RUTA_ARCHIVO $(basename ${vFTP_NOM_ARCHIVO_FORMATO})\n"

#REALIZA LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo  "==== Inicia exportacion del archivo TXT a servidor SFTP ====" 2>&1 &>> $VAL_LOG
echo "Host SFTP: $VAL_SFTP_HOSTNAME" 2>&1 &>> $VAL_LOG
echo "Puerto SFTP: $VAL_SFTP_PUERTO" 2>&1 &>> $VAL_LOG
echo "Usuario SFTP: $VAL_SFTP_USER" 2>&1 &>> $VAL_LOG
echo "Password SFTP: $VAL_SFTP_PASS" 2>&1 &>> $VAL_LOG
echo "Ruta SFTP: $VAL_SFTP_RUTA" 2>&1 &>> $VAL_LOG
exportar $VAL_NOM_ARCHIVO 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DE LA TRANSFERENCIA DEL ARCHIVO TXT A RUTA FTP
echo "==== Valida transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
VAL_ERROR_FTP=`egrep 't be established|Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
	if [ $VAL_ERROR_FTP -ne 0 ]; then
		echo "==== ERROR - En la transferencia del archivo TXT al servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
		exit 1
		else
		echo "==== OK - La transferencia del archivo TXT al servidor FTP es EXITOSA ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	fi
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params_des
echo "==== OK - Se procesa la ETAPA 3 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params_des set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
	
echo "==== Finaliza ejecucion del proceso ALTAS APP TUENTI ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG

--CREA NUEVA TABLA otc_t_tuenti_account RAW PARTICIONADA EN HIVE db_tuenti
CREATE TABLE db_desarrollo2021.otc_t_tuenti_account(
id_account string COMMENT 'identificador de la cuenta',
name string COMMENT 'nombre',
last_name string COMMENT 'apellido',
email string COMMENT 'email',
password string COMMENT 'contraseña',
terms_accepted string COMMENT 'terminos aceptados',
social_id string COMMENT 'identificador social',
platform string COMMENT 'plataforma',
token string COMMENT 'token',
create_date timestamp COMMENT 'fecha de creacion',
source string COMMENT 'fuente',
app_version string COMMENT 'version de la aplicacion',
platform_version string COMMENT 'version de la plataforma',
browser string COMMENT 'navegador',
browser_version string COMMENT 'version del navegador',
media_source string COMMENT 'fuente media',
last_login timestamp COMMENT 'fecha ultimo ingreso',
imei string COMMENT 'imei',
device_model string COMMENT 'modelo del dispositivo',
device_manufacturer string COMMENT 'fabricante del dispositivo',
nickname string COMMENT 'alias',
gender string COMMENT 'genero',
birthday string  COMMENT 'cumpleaños'
)
COMMENT 'Tabla particionada en Hive con la informacion de las cuentas de tuenti'
PARTITIONED BY (pt_mes bigint);

--CREA NUEVA TABLA otc_t_tuenti_msisdn_by_account RAW PARTICIONADA EN HIVE db_tuenti
CREATE TABLE db_desarrollo2021.otc_t_tuenti_msisdn_by_account(
id_msisdn_by_account string COMMENT 'identificador del msisdn para la cuenta',
id_account string COMMENT 'identificador de la cuenta',
msisdn string COMMENT 'msisdn',
notification_push string COMMENT 'notificacion push',
create_date timestamp COMMENT 'fecha de creacion',
agent string COMMENT 'nombre agente',
agent_id string COMMENT 'identificador del agente',
alias string COMMENT 'alias',
status string COMMENT 'estado',
token_push string COMMENT 'token push'
)
COMMENT 'Tabla particionada en Hive con la informacion de los msisdn para las cuentas de tuenti'
PARTITIONED BY (pt_mes bigint)
STORED AS ORC
TBLPROPERTIES ('external.table.purge'='true');

--CREA NUEVA TABLA DESTINO PARTICIONADA EN HIVE db_reportes
CREATE TABLE db_desarrollo2021.otc_t_altas_app_tuenti(
fecha_alta timestamp COMMENT 'fecha de alta',
nombre string COMMENT 'nombre y apellido',
msisdn string COMMENT 'msisdn',
id_hash string COMMENT 'identificador unico de la tabla',
fecha_proceso string COMMENT 'ultimo dia del mes de proceso' 
)
COMMENT 'Tabla particionada en Hive con la informacion de altas app con tuenti'
PARTITIONED BY (pt_mes bigint)
STORED AS ORC
TBLPROPERTIES ('external.table.purge'='true');

--HACE EL VOLVADO DE LA HISTORIA DE LAS TABLAS ANTERIORMENTE CREADAS
INSERT INTO db_tuenti.otc_t_tuenti_account PARTITION(pt_mes)
SELECT * FROM db_desarrollo2021.otc_t_tuenti_account;

INSERT INTO db_tuenti.otc_t_tuenti_msisdn_by_account PARTITION(pt_mes)
SELECT * FROM db_desarrollo2021.otc_t_tuenti_msisdn_by_account;

INSERT INTO db_reportes.otc_t_altas_app_tuenti PARTITION(pt_mes)
SELECT * FROM db_desarrollo2021.otc_t_altas_app_tuenti;


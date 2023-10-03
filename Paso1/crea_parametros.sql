+-----------------+---------------------+----------------------------------+-------+----------+
| ENTIDAD         | PARAMETRO           | VALOR                            | ORDEN | AMBIENTE |
+-----------------+---------------------+----------------------------------+-------+----------+
| URMTNTACCNT0010 | VAL_RUTA            | /RAW/TUENTI/OTC_T_TUENTI_ACCOUNT |     0 |        1 |
| URMTNTACCNT0010 | VAL_ESQUEMA         | db_tuenti                        |     0 |        1 |
| URMTNTACCNT0010 | VAL_TABLA           | otc_t_tuenti_account             |     0 |        1 |
| URMTNTACCNT0010 | VAL_CAMPO_PARTICION | pt_mes                           |     0 |        1 |
+-----------------+---------------------+----------------------------------+-------+----------+

+-----------------+---------------------+--------------------------------------------+-------+----------+
| ENTIDAD         | PARAMETRO           | VALOR                                      | ORDEN | AMBIENTE |
+-----------------+---------------------+--------------------------------------------+-------+----------+
| URMMSISDNBY0020 | VAL_RUTA            | /RAW/TUENTI/OTC_T_TUENTI_MSISDN_BY_ACCOUNT |     0 |        1 |
| URMMSISDNBY0020 | VAL_ESQUEMA         | db_tuenti                                  |     0 |        1 |
| URMMSISDNBY0020 | VAL_TABLA           | otc_t_tuenti_msisdn_by_account             |     0 |        1 |
| URMMSISDNBY0020 | VAL_CAMPO_PARTICION | pt_mes                                     |     0 |        1 |
+-----------------+---------------------+--------------------------------------------+-------+----------+

+-----------------------+-----------------------+---------------------------------------------------------------------+-------+----------+
| ENTIDAD               | PARAMETRO             | VALOR                                                               | ORDEN | AMBIENTE |
+-----------------------+-----------------------+---------------------------------------------------------------------+-------+----------+
| EXTRALTSAPPTUENTI0020 | PARAM1_FECHA          | 20230704                                                            |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | PARAM2_VAL_RUTA       | '/RGenerator/reportes/ALTAS_APP_TUENTI'                             |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | ETAPA                 | 1                                                                   |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_FTP_PUERTO        | 22                                                                  |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_FTP_USER          | telefonicaecuadorprod                                               |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_FTP_HOSTNAME      | ftp.cloud.varicent.com                                              |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_FTP_PASS          | RqiZ2lkJmeiQTi2hvRwd                                                |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_FTP_RUTA          | /Data                                                               |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_NOM_ARCHIVO       | Extractor_Tuenti_APP.txt                                            |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | SHELL                 | /RGenerator/reportes/ALTAS_APP_TUENTI/bin/OTC_T_ALTAS_APP_TUENTI.sh |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_RUTA_SPARK        | /usr/hdp/current/spark2-client/bin/spark-submit                     |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_ESQUEMA_RAW       | db_tuenti                                                           |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_ESQUEMA_HIVE      | db_reportes                                                         |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_TABLA_RAW_ACCOUNT | otc_t_tuenti_account                                                |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_TABLA_RAW_MSISDN  | otc_t_tuenti_msisdn_by_account                                      |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_TABLA_TUENTI      | otc_t_altas_app_tuenti                                              |     0 |        1 |
| EXTRALTSAPPTUENTI0020 | VAL_CAMPO_PT          | pt_mes                                                              |     0 |        1 |
+-----------------------+-----------------------+---------------------------------------------------------------------+-------+----------+

--PARAMETROS PARA LA ENTIDAD D_URMTNTACCNT0010
DELETE FROM params_des WHERE entidad='D_URMTNTACCNT0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','ETAPA','1',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_RUTA','/RAW/TUENTI/OTC_T_TUENTI_ACCOUNT',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','SHELL','/RAW/TUENTI/OTC_T_TUENTI_ACCOUNT/bin/OTC_T_TUENTI_ACCOUNT.sh',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_ESQUEMA','db_tuenti',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_TABLA','otc_t_tuenti_account',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_CAMPO_PARTICION','pt_mes',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_HOST','10.112.152.230',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_PORT','5432',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_DB','otc_tuenti',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_DRIVER','org.postgresql.Driver',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_USUARIO','NABPTNTI001',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_MASTER','local','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_DRIVER_MEMORY','32G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_EXECUTOR_MEMORY','32G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_NUM_EXECUTORS','8','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_NUM_EXECUTORS_CORES','4','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMTNTACCNT0010','VAL_QUEUE','capa_semantica','0','1');
SELECT * FROM params_des WHERE ENTIDAD='D_URMTNTACCNT0010';

--PARAMETROS PARA LA ENTIDAD D_URMMSISDNBY0020
DELETE FROM params_des WHERE entidad='D_URMMSISDNBY0020';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','ETAPA','1',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_RUTA','/RAW/TUENTI/OTC_T_TUENTI_MSISDN_BY_ACCOUNT',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','SHELL','/RAW/TUENTI/OTC_T_TUENTI_MSISDN_BY_ACCOUNT/bin/OTC_T_TUENTI_MSISDN_BY_ACCOUNT.sh',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_ESQUEMA','db_tuenti',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_TABLA','otc_t_tuenti_msisdn_by_account',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_CAMPO_PARTICION','pt_mes',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_HOST','10.112.152.230',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_PORT','5432',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_DB','otc_tuenti',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_DRIVER','org.postgresql.Driver',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_USUARIO','NABPTNTI001',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_MASTER','local','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_DRIVER_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_NUM_EXECUTORS','4','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_NUM_EXECUTORS_CORES','4','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_URMMSISDNBY0020','VAL_QUEUE','capa_semantica','0','1');
SELECT * FROM params_des WHERE ENTIDAD='D_URMMSISDNBY0020';

--PARAMETROS PARA LA ENTIDAD D_EXTRALTSAPPTUENTI0020
DELETE FROM params_des WHERE entidad='D_EXTRALTSAPPTUENTI0020';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','PARAM1_FECHA','20230704',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','PARAM2_VAL_RUTA','/RGenerator/reportes/ALTAS_APP_TUENTI',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','ETAPA','1',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','SHELL','/RGenerator/reportes/ALTAS_APP_TUENTI/bin/OTC_T_ALTAS_APP_TUENTI.sh',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_SFTP_PUERTO','22',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_SFTP_USER','telefonicaecuadorprod',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_SFTP_HOSTNAME','ftp.cloud.varicent.com',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_SFTP_PASS','RqiZ2lkJmeiQTi2hvRwd','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_SFTP_RUTA','/Data','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_NOM_ARCHIVO','Extractor_Tuenti_APP.txt','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_ESQUEMA_RAW','db_tuenti','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_ESQUEMA_HIVE','db_reportes','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_TABLA_RAW_ACCOUNT','otc_t_tuenti_account','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_TABLA_RAW_MSISDN','otc_t_tuenti_msisdn_by_account','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_TABLA_TUENTI','otc_t_altas_app_tuenti','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_CAMPO_PT','pt_mes','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRALTSAPPTUENTI0020','VAL_QUEUE','capa_semantica','0','1');
SELECT * FROM params_des WHERE ENTIDAD='D_EXTRALTSAPPTUENTI0020';

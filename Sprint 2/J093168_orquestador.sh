#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - Staging
### $6 : Ruta Log TERADATA
### $7 : Ruta Shell TERADATA
### $8 : Periodo
### $9 : Fecha sp1
### $10: Fecha sp2
## sh J093168_orquestador.sh tdsunat usr_carga_prod twusr_carga_prod bddwedq bddwestg bddwetb dwh_data /work1/teradata/log/093168 /work1/teradata/shells/093168 2022 2023-02-06 2022-10-01 2023-02-08
## sh J093168_orquestador.sh tdtp01s2 usr_carga_desa twusr_carga_desa bddwedqd bddwestgd bddwetbd desa_dwh_data /work1/teradata/log/093168 /work1/teradata/shells/093168 2022 2023-02-06 2022-10-01 2023-02-08
################################################################################


if [ $# -ne 13 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi

### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
BD_TB=${6}
DW_DATA=${7}
path_log_TD=${8}
path_shell_TD=${9}
PERIODO=${10}
FECHA_SP1=${11}
FECHA_SP2=${12}
FECHA_FV=${13}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'

###############################################################SPRINT 1###############################################################################

sh ${path_shell_TD}/J093168_CalculateIndPresDJ.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${path_log_TD} ${PERIODO}  ${FECHA_FV}
if [ $? -ne 0 ]; then echo "J093168_CalculateIndPresDJ|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_CalculateIndPresDJ|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP01.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_TB} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP01|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP01|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP02.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_TB} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP02|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP02|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP03.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_TB} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP03|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP03|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP04.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_TB} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP04|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP04|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP05.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_TB} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP05|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP05|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP06.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${BD_TB} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP06|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP06|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP07.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${BD_TB} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP07|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP07|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP08.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${BD_TB} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP08|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP08|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP09.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${BD_TB} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP09|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP09|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP10.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${BD_TB}  ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP10|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP10|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP11.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${BD_TB} ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP11|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP11|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP12.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_TB} ${BD_LND}  ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP12|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP12|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP13.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_TB} ${BD_LND}  ${path_log_TD} ${PERIODO} ${FECHA_SP1}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP13|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP13|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

######################################################SPRINT 2 ##########################################################################################

sh ${path_shell_TD}/J093168_KPIGRP14VAL.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP14VAL|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP14VAL|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP14OBS.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP14OBS|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP14OBS|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP15.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP15|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP15|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP16VAL.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP16VAL|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP16VAL|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP16OBS.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP16OBS|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP16OBS|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP17.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO} 
if [ $? -ne 0 ]; then echo "J093168_KPIGRP17|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP17|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP018.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP018|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP018|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP019.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP019|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP019|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP020.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${path_log_TD} ${PERIODO} 
if [ $? -ne 0 ]; then echo "J093168_KPIGRP020|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP020|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP021.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP021|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP021|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP022.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP022|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP022|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP023.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP023|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP023|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP024.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP024|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP024|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP025.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND} ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP025|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP025|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP2627.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_LND}  ${path_log_TD} ${PERIODO}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP2627|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP2627|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

sh ${path_shell_TD}/J093168_KPIGRP36.sh ${server_TD} ${username_TD} ${walletPwd_TD} ${BD_DQ} ${BD_STG} ${BD_TB} dwh_data ${path_log_TD} ${PERIODO} ${FECHA_SP2}
if [ $? -ne 0 ]; then echo "J093168_KPIGRP36|err|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; else echo "J093168_KPIGRP36|ok|`date +%Y%m%d+%H%M%S`" >>${FILELOG}; fi

tail -50 ${FILELOG}
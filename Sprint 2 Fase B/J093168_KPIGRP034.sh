#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server Informix
### $2 : Base de datos Informix
### $3 : User Informix
### $4 : Password Informix
### $5 : Informix Dir SQLhosts
### $6 : Server Teradata
### $7 : Usuario Teradata
### $8 : Wallet Teradata
### $9 : Base de datos Teradata - DQ
### $10 : Base de datos Teradata - Staging
### $11 : Base de datos Teradata - Landing
### $12 : Ruta log shell
### $13 : PERIODO
##  sh /work1/teradata/shells/093168/J093168_KPIGRP034.sh serverifx bdifx userifx passwordifx pathifx tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG BDDWELND /work1/teradata/log/093168 2022
##  sh /work1/teradata/shells/093168/J093168_KPIGRP034.sh serverifx bdifx userifx passwordifx pathifx tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD BDDWETBD /work1/teradata/log/093168 2022
################################################################################


if [ $# -ne 13 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi

### PARAMETROS

IFX_SERVER=${1}
IFX_BD=${2}
IFX_USER=${3}
IFX_PASSWORD=${4}
IFX_PATH=${5}
server_TD=${6}
username_TD=${7}
walletPwd_TD=${8}
BD_DQ=${9}
BD_STG=${10}
BD_TB=${11}
path_log_TD=${12}
PERIODO=${13}


INFORMIXDIR=${IFX_PATH}
INFORMIXSERVER=${IFX_SERVER}
DBACCNOIGN=1
export DBACCNOIGN
export INFORMIXSERVER INFORMIXDIR
PATH=${PATH}:${IFX_PATH}/bin


exec </dev/null 

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILESQL=${path_log_TD}'/'IFX_${NOMBREBASE}.sql 
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K034012022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_'${DATE}'.unl'
plano=${path_log_TD}'/tmp093168_'${KPI_01}'_t4241cabcpe.unl'


rm -f ${FILE_KPI01}


###========================================================================================= */
###**********************************Cantidad CPE Baja Informix  ******************************/
###========================================================================================= */

query1="
CONNECT TO '@${IFX_SERVER}' USER '${IFX_USER}' USING '${IFX_PASSWORD}';
DATABASE  ${IFX_BD};

set isolation to dirty read;

unload to '${plano}' delimiter '|'
select count(1) as cnt_reg from t4241cabcpe
Where ind_estado = 2 and 
cod_cpe not in ('04','14');
"

echo ${query1}>${FILESQL}

dbaccess -e -a ${IFX_BD} ${FILESQL} >/dev/null 2>${FILELOG}

if [ $? -ne 0 ]; then
    echo ' '
    echo '  ***********************************************' >> ${FILEMSG}
    echo '  *** ' ${NOMBREBASE}  '  ***' >> ${FILEMSG}
    echo '  ***       ERROR!!!! EN PROCESO INFORMIX     ***' >> ${FILEMSG}
    echo '  ***********************************************' >> ${FILEMSG}
    cat ${FILEMSG} >> ${FILELOG}
    cat ${FILEMSG}
    echo 'Revisar la ejecucion de la shell en los siguientes archivos:'
    echo ${FILELOG}
    echo ${FILEERR}
    echo ' '
    echo '######  < Inicio >  ########################################################'
    echo ' '
    echo '     ######  I.  VER ERROR #################################################'
    cat ${FILEERR}
    echo '     ######  II. VER LOG   #################################################'
    cat ${FILELOG}
    echo '######  -  Fin -  ##########################################################'
    rm ${FILEMSG}
    exit 1
fi

bteq <<EOF>>${FILELOG} 2>${FILEERR}

.SESSIONS 4
LOGON ${LOGONDB};
DATABASE ${BD_DQ};
    .SET DEFAULTS
    .SET TITLEDASHES OFF
    .SET WIDTH 32000


SEL CURRENT_TIMESTAMP;


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr34_cantorigen';
.IF activitycount = 0 THEN .GOTO ok 
DROP TABLE ${BD_STG}.tmp093168_kpigr34_cantorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;
.label ok;


CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr34_cantorigen
(
cnt_origen    varchar(20)
) NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.IMPORT VARTEXT '|'  FILE = '${plano}'
.QUIET ON
.REPEAT *
USING 
(
cnt_origen varchar(20)
)
INSERT INTO  ${BD_STG}.tmp093168_kpigr34_cantorigen VALUES
(
:cnt_origen
);

.QUIET OFF;


SEL CURRENT_TIMESTAMP;

/*========================================================================================= */
/**********************************Cantidad CPE Baja Teradata  ******************************/
/*========================================================================================= */


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr34_cantdestino';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr34_cantdestino;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr34_cantdestino
AS
(
    SELECT COUNT(*) as cnt_destino
    FROM ${BD_TB}.t7630comprobcabe
    WHERE fec_finvig=2000101 and 
    ind_deldwe='0' and
    ind_estado_cpe='2' and
    COD_COMPROBANTE not in ('04','14')

) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;




/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_01}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
    INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)--,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
    SELECT 
        '${PERIODO}',
        99,
        '${KPI_01}',
        CURRENT_DATE,
        (select sum(cnt_origen) from ${BD_STG}.tmp093168_kpigr34_cantorigen),---denominador
        (select sum(cnt_destino) from ${BD_STG}.tmp093168_kpigr34_cantdestino)--numerador
    ;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

/********************************************************************************/

SEL CURRENT_TIMESTAMP;

    
LOGOFF;
QUIT 0;
.LABEL error_shell
LOGOFF;
QUIT 8;


EOF

CODRET=$?

FILEMSG=msg.log

if [ $CODRET -ne 0 ]; then
    echo ' '
    echo '  ***********************************************' >> ${FILEMSG}
    echo '  *** ' ${NOMBREBASE}  '  ***' >> ${FILEMSG}
    echo '  ***           ERROR!!!! EN PROCESO          ***' >> ${FILEMSG}
    echo '  ***********************************************' >> ${FILEMSG}
    cat ${FILEMSG} >> ${FILELOG}
    cat ${FILEMSG}
    echo 'Revisar la ejecucion de la shell en los siguientes archivos:'
    echo ${FILELOG}
    echo ${FILEERR}
    echo ' '
    echo '######  < Inicio >  ########################################################'
    echo ' '
    echo '     ######  I.  VER ERROR #################################################'
    cat ${FILEERR}
    echo '     ######  II. VER LOG   #################################################'
    cat ${FILELOG}
    echo '######  -  Fin -  ##########################################################'
    rm ${FILEMSG}
    exit 1
fi

if [ $CODRET = 0 ]
then
    echo ' '
    echo '  ***********************************************' >> ${FILEMSG}
    echo '  *** ' ${NOMBREBASE}  '  ***' >> ${FILEMSG}
    echo '  ***            TERMINO PROCESO OK           ***' >> ${FILEMSG}
    echo '  ***********************************************' >> ${FILEMSG}
    cat ${FILEMSG} >> ${FILELOG}
    cat ${FILEMSG}
    echo 'Revisar la ejecucion de la shell en los siguientes archivos:'
    echo ${FILELOG}
    echo ${FILEERR}
    echo ' '
    echo '######  < Inicio >  ########################################################'
    echo ' '
    echo '     ######  I.  VER ERROR #################################################'
    cat ${FILEERR}
    echo '     ######  II. VER LOG   #################################################'
    cat ${FILELOG}
    echo '######  -  Fin -  ##########################################################'
    rm ${FILEMSG}
    exit 0
fi
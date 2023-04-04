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
##  sh /work1/teradata/shells/093168/J093168_KPIGRP031_2.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG BDDWETB /work1/teradata/log/093168 2022
##  sh /work1/teradata/shells/093168/J093168_KPIGRP031_2.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD BDDWETBD /work1/teradata/log/093168 2022
################################################################################


if [ $# -ne 8 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi

### PARAMETROS


server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
BD_TB=${6}
path_log_TD=${7}
PERIODO=${8}


MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K031022022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_'${DATE}'.unl'


rm -f ${FILE_KPI01}

bteq <<EOF>>${FILELOG} 2>${FILEERR}

LOGON ${LOGONDB};

DATABASE ${BD_DQ};
    .SET DEFAULTS
    .SET TITLEDASHES OFF
    .SET WIDTH 32000


SEL CURRENT_TIMESTAMP;


/*========================================================================================= */
/***********Cantidad de registros en Teradata ITF Extornos  ******************************/
/*========================================================================================= */


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr31_02_cantdestino';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr31_02_cantdestino;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr31_02_cantdestino
AS
(
    SELECT COUNT(1) as cnt_destino
    from ${BD_TB}.T8477ITFEXT x0
    where x0.fec_finvig=2000101 and x0.ind_deldwe='0'
    and x0.per_dec between '${PERIODO}01' and '${PERIODO}12'
    and x0.num_formul='0695'
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;




/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_01}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
    INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
    SELECT 
        '${PERIODO}',
        99,
        '${KPI_01}',
        CURRENT_DATE,
        (select sum(cnt_origen) from ${BD_STG}.tmp093168_kpigr31_02_cantorigen),---denominador
        (select sum(cnt_destino) from ${BD_STG}.tmp093168_kpigr31_02_cantdestino)--numerador
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
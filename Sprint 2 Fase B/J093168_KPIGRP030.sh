#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - Staging
### $6 : Base de datos Teradata - Landing
### $7 : Ruta Log TERADATA
##  sh /work1/teradata/shells/093168/J093168_KPIGRP030.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG BDDWELND /work1/teradata/log/093168 2022
##  sh /work1/teradata/shells/093168/J093168_KPIGRP030.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD DESA_DWH_DATA /work1/teradata/log/093168 2022
################################################################################


if [ $# -ne 8 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
BD_DWH=${6}
path_log_TD=${7}
PERIODO=${8}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K030012022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_'${DATE}'.unl'


rm -f ${FILE_KPI01}


bteq <<EOF>${FILELOG} 2>${FILEERR}

LOGON ${LOGONDB};

DATABASE ${BD_DQ};

.SET FORMAT OFF;
.SET WIDTH 32000;
.SET SEPARATOR '|';
.SET TITLEDASHES OFF;

SEL CURRENT_TIMESTAMP;

/*========================================================================================= */
/**********************************Cantidad CIC Total   *************************************/
/*========================================================================================= */

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr30_universocic';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr30_universocic;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr30_universocic AS (
select cod_fuente,cod_tipidenti,num_docidenti,fec_finvig,fec_inivig,cod_cic
,des_apepat,des_apemat,des_nompri,des_nomrazsoc,fec_nacimiento
from ${BD_DWH}.t2017identif
where fec_finvig=2000101
) WITH DATA NO PRIMARY INDEX;



SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr30_detcic_full';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr30_detcic_full;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr30_detcic_full
AS
(

    with tmp093168_kpigr30_detcic_total as
    (
     select distinct cod_cic, des_nomrazsoc, fec_nacimiento
     from ${BD_STG}.tmp093168_kpigr30_universocic
    ), tmp093168_kpigr30_group_desrazsoc as
    (
        select des_nomrazsoc, fec_nacimiento,count(*) as cant
        from tmp093168_kpigr30_detcic_total
        group by 1,2
        having cant>1
    ), tmp093168_kpigr30_group_cic as
    (
        select cod_cic,count(*) as cant
        from tmp093168_kpigr30_detcic_total
        group by 1
        having cant>1
    ) 
    select x0.*,
           case when x1.des_nomrazsoc is not null then 1
                 when x2.cod_cic is not null then 1
            else 0 end flag_error
    from   ${BD_STG}.tmp093168_kpigr30_universocic x0
    left join tmp093168_kpigr30_group_desrazsoc x1 on  x0.des_nomrazsoc=x1.des_nomrazsoc and x0.fec_nacimiento=x1.fec_nacimiento
    left join tmp093168_kpigr30_group_cic x2 on x0.cod_cic=x2.cod_cic
) WITH DATA NO PRIMARY INDEX;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/   


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}    ;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

 CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
    SELECT *
    FROM ${BD_STG}.tmp093168_kpigr30_detcic_full
    WHERE flag_error=1
 )  WITH DATA NO PRIMARY INDEX;

 .IF ERRORCODE <> 0 THEN .GOTO error_shell;


/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_01}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
    INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
    SELECT 
        '${PERIODO}',
        99,
        '${KPI_01}',
        CURRENT_DATE,
        (select count(*) from ${BD_STG}.tmp093168_kpigr30_detcic_full),---denominador
        (select count(*) from ${BD_STG}.tmp093168_kpigr30_detcic_full where flag_error=0),--numerador
        case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 and
                   (select count(*) from ${BD_STG}.tmp093168_kpigr30_detcic_full)<>0)
        then 1 else 0 end,
        (select count(*) from  ${BD_STG}.tmp093168_dif_${KPI_01}),
         0,
        (select count(*) from ${BD_STG}.tmp093168_kpigr30_detcic_full where flag_error=0)
    ;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*******************************************************************************/

 .EXPORT FILE ${FILE_KPI01};

	LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01} 
	ORDER BY cod_cic;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

 .EXPORT RESET;

/********************************************************************************/

	DROP TABLE ${BD_STG}.tmp093168_kpigr30_detcic_full;

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
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
##  sh /work1/teradata/shells/093168/J093168_KPIGRP028.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG BDDWELND /work1/teradata/log/093168 2022
##  sh /work1/teradata/shells/093168/J093168_KPIGRP028.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD BDDWELNDD /work1/teradata/log/093168 2022
################################################################################


if [ $# -ne 8 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
BD_LND=${6}
path_log_TD=${7}
PERIODO=${8}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K028012022'
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
/**********************************Contribuyentes TOTAL,CII4*********************************/
/*========================================================================================= */

------------------------Contribuyentes total-------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr28_detruc_total';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr28_detruc_total;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr28_detruc_total
AS
(
SELECT  DISTINCT TRIM(ddp_numruc) as num_ruc FROM ${BD_LND}.DDP_RUC
) WITH DATA UNIQUE PRIMARY INDEX( num_ruc);

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

------------------------Contribuyentes CIIU 4 -----------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr28_detruc_ciiu4';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr28_detruc_ciiu4;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr28_detruc_ciiu4
AS (
SELECT DISTINCT TRIM(num_ruc) as num_ruc
FROM ${BD_LND}.t5667acteco WHERE cod_tipact='P'
) WITH DATA UNIQUE PRIMARY INDEX (num_ruc);

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 



/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/	

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_01}
AS
(
    SELECT x0.num_ruc,x1.num_ruc as num_rucB
    FROM ${BD_STG}.tmp093168_kpigr28_detruc_total x0
	FULL JOIN ${BD_STG}.tmp093168_kpigr28_detruc_ciiu4 x1
    ON x0.num_ruc=x1.num_ruc
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;
	
SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	
	
 CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
    SELECT * FROM ${BD_STG}.tmp093168_total_${KPI_01} x0
    WHERE x0.num_rucB is null 
 )  WITH DATA PRIMARY INDEX (num_ruc);

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
		(select count(*) from ${BD_STG}.tmp093168_kpigr28_detruc_total),
		(select count(*) from ${BD_STG}.tmp093168_kpigr28_detruc_ciiu4),
		case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0  and 
                  (select count(*) from ${BD_STG}.tmp093168_kpigr28_detruc_total)<>0)
                  then 1 else 0 end,
		(select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}),
        (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc is null),
		(select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc=num_rucB)
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/*****************************************************************************/
 .EXPORT FILE ${FILE_KPI01};

	LOCK ROW FOR ACCESS
	SELECT num_ruc FROM ${BD_STG}.tmp093168_dif_${KPI_01} 
	ORDER BY num_ruc;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

 .EXPORT RESET;


/********************************************************************************/

	DROP TABLE ${BD_STG}.tmp093168_kpigr28_detruc_total;
	DROP TABLE ${BD_STG}.tmp093168_kpigr28_detruc_ciiu4;

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
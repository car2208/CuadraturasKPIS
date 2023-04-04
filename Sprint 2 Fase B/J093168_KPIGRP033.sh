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
##  sh /work1/teradata/shells/093168/J093168_KPIGRP033.sh tdsunat usr_carga_prod twusr_carga_prod BDDWETB BDDWEDQ BDDWESTG BDDWELND /work1/teradata/log/093168 2022
##  sh /work1/teradata/shells/093168/J093168_KPIGRP033.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWETBD BDDWEDQD BDDWESTGD BDDWELNDD /work1/teradata/log/093168 2022
################################################################################


if [ $# -ne 9 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_TB=${4}
BD_DQ=${5}
BD_STG=${6}
BD_LND=${7}
path_log_TD=${8}
PERIODO=${9}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K033012022'
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


/************************************************************************************************************/
---------------Cantidad de presentaciones en Recauda. t03djcab Y t04djdet. CAS 406 426.---------------------------
/************************************************************************************************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr33_cas406_426_djtot';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr33_cas406_426_djtot;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;


.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr33_cas406_426_djtot
AS
(
    select x0.num_nabono,
           x0.cod_formul,
           x0.num_orden,
           x0.num_ruc,
           x0.cod_per,
          coalesce(trycast(x1.val_cas as decimal(25,4)),0) as cas406,
          coalesce(trycast(x1.val_cas as decimal(25,4)),0) as cas426,
          (cas406+cas426) val_cas
    from ${BD_TB}.t8593djcab x0
    inner join ${BD_TB}.t8594djdet x1 ON 
    x0.num_nabono=x1.num_nabono and 
    x0.cod_formul=x1.cod_formul and 
    x0.num_orden=x1.num_orden
    where x1.num_cas in('406','426') and
    x0.fec_finvig=2000101 and
    x1.fec_finvig=2000101 and
    x0.ind_deldwe='0' and
    x1.ind_deldwe='0' and
    x0.cod_per between '${PERIODO}01' and '${PERIODO}12' and
    x0.cod_formul='0695'
) WITH DATA UNIQUE PRIMARY INDEX (num_nabono,cod_formul,num_orden);

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/************************************************************************************************************/
----------------Cantidad de presentaciones en el Recauda.T1391F695EXTITF. Monto Base -------------------------
/************************************************************************************************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr33_mtobase_detextitf';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr33_mtobase_detextitf;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr33_mtobase_detextitf
AS
(
select num_nabono,num_formul as cod_formul,num_orden,num_docdeclate,per_dec,sum(mto_base) as mto_base
from ${BD_TB}.t8477itfext x0
where x0.fec_finvig=2000101 and x0.ind_deldwe='0'
and x0.per_dec between '${PERIODO}01' and '${PERIODO}12'
and x0.num_formul='0695'
group by 1,2,3,4,5
) WITH DATA UNIQUE PRIMARY INDEX (num_nabono,cod_formul,num_orden);

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


-----------------------------------------------Diferencias ------------------------------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_01}
AS
(
SELECT 
     x0.num_nabono,
     x0.cod_formul,
     x0.num_orden,
     x0.num_ruc,
     x0.cod_per,
     x0.val_cas,
     x1.mto_base,
     x1.num_docdeclate as num_rucB
FROM ${BD_STG}.tmp093168_kpigr33_cas406_426_djtot x0
FULL JOIN ${BD_STG}.tmp093168_kpigr33_mtobase_detextitf x1 ON
x0.num_nabono=x1.num_nabono and
x0.cod_formul=x1.cod_formul and
x0.num_orden=x1.num_orden and
x0.num_ruc=x1.num_docdeclate and 
x0.cod_per=x1.per_dec and
x0.val_cas=x1.mto_base
) WITH DATA PRIMARY INDEX (num_nabono,cod_formul,num_orden);

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;	
 CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
  SELECT x0.num_nabono,
         x0.cod_formul,
         x0.num_orden,
         x0.num_ruc,
         x0.cod_per,
         x0.val_cas  as val_mtobase_t04,
         x0.mto_base as val_mtobase_itfext
    FROM ${BD_STG}.tmp093168_total_${KPI_01} x0
    WHERE x0.num_rucB is null 
 )  WITH DATA NO PRIMARY INDEX;

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
        (select count(*) from ${BD_STG}.tmp093168_kpigr33_cas406_426_djtot),
        (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where val_cas=mto_base),
        case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 and
                  (select count(*) from ${BD_STG}.tmp093168_kpigr33_cas406_426_djtot)<>0)
        then 1 else 0 end,
		(select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}),
        (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc is null),
		(select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc=num_rucB);

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/**********************************************************************************/

 .EXPORT FILE ${FILE_KPI01};

	LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01} 
	ORDER BY 3,4;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

 .EXPORT RESET;

/********************************************************************************/

	DROP TABLE ${BD_STG}.tmp093168_kpigr33_mtobase_detextitf;
	DROP TABLE ${BD_STG}.tmp093168_kpigr33_cas406_426_djtot;

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
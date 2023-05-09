#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - TB
### $6 : Base de datos Teradata - DWH_DATA
### $8 : Ruta Log TERADATA
### $9 : Periodo
### $10 : Fecha
### sh /work1/teradata/shells/093168/J093168_KPIGRP36.sh tdsunat usr_carga_prod twusr_carga_prod bddwedq bddwestg bddwetb dwh_data /work1/teradata/log/093168 2022 2022-10-01
### sh /work1/teradata/shells/093168/J093168_KPIGRP36.sh tdtp01s2 usr_carga_desa twusr_carga_desa bddwedqd bddwestgd bddwetbd desa_dwh_data /work1/teradata/log/093168 2022 2022-10-01
 
################################################################################

if [ $# -ne 10 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
BD_TB=${6}
DW_DATA=${7}
path_log_TD=${8}
PERIODO=${9}
FECHA=${10}

### INCLUSION DE RUTINAS GENERALES

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K036012022'
plano='/work1/teradata/dat/093168/093168_KPI_ListFormularios.txt'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_TRANVSMODB_'${DATE}'.unl'

rm -f ${FILE_KPI01}

bteq <<EOF>${FILELOG} 2>${FILEERR}

.SESSIONS 4

LOGON ${LOGONDB};

DATABASE ${BD_DQ};

	.SET DEFAULTS
	.SET TITLEDASHES OFF
	.SET WIDTH 1000


SEL CURRENT_TIMESTAMP;


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi_formularios';
.IF activitycount = 0 THEN .GOTO ok 
DROP TABLE ${BD_STG}.tmp093168_kpi_formularios;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;
.label ok;


CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi_formularios
(
cod_form    varchar(4)
) UNIQUE PRIMARY INDEX (cod_form);

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.IMPORT VARTEXT '|'  FILE = '${plano}'
.QUIET ON
.REPEAT *
USING 
(
cod_form       varchar(4)
)
INSERT INTO  ${BD_STG}.tmp093168_kpi_formularios VALUES
(
:cod_form
);

.QUIET OFF;


SEL CURRENT_TIMESTAMP;



------------Genera Detalle Presentaciones Teradata-------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi36_detafo_tr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi36_detafo_tr;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi36_detafo_tr
AS
(
select
    num_docidenti,
    num_nabono,
    cod_formul,
    num_orden,    
    fec_presenta,
    num_periodo
from ${DW_DATA}.t2782djcab
where fec_presenta >= CAST('${FECHA}' AS DATE FORMAT 'YYYY-MM-DD')
and cod_formul in (SELECT CAST(cod_form as integer) FROM ${BD_STG}.tmp093168_kpi_formularios)
and num_periodo between ${PERIODO}01 and ${PERIODO}13
)WITH DATA PRIMARY INDEX (num_nabono,cod_formul,num_orden);

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

------------Genera Detalle Presentaciones MongoDB-------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi36_detafo_mdb';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi36_detafo_mdb;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi36_detafo_mdb
AS
(
select 
    numRuc,
    numOperacion,
    codFormulario,
    numOrden,    
    cast(substr(trim(fecPresentacion),1,10) as date format 'yyyy-mm-dd') as fec_presenta,
    perPeriodo
 from ${BD_STG}.present_mongo2 
 where fec_presenta >= CAST('${FECHA}' AS DATE FORMAT 'YYYY-MM-DD')
 and codFormulario in (SELECT cast(cod_form as integer) FROM ${BD_STG}.tmp093168_kpi_formularios)
 and perPeriodo between '${PERIODO}01' and '${PERIODO}13'

)
WITH DATA PRIMARY INDEX (numOperacion,codFormulario,numOrden);

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr36_obs_cnorigen';
.IF activitycount = 0 THEN .GOTO ok 
DROP TABLE ${BD_STG}.tmp093168_kpigr36_obs_cnorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;
.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr36_obs_cnorigen AS
(
    SELECT count(num_orden) as cant_comp_origen
    FROM ${BD_STG}.tmp093168_kpi36_detafo_tr
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

---------2. Conteo en MongoDB


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr36_cndestino1';
.IF activitycount = 0 THEN .GOTO ok 
DROP TABLE ${BD_STG}.tmp093168_kpigr36_cndestino1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;
.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr36_cndestino1 AS
(
    SELECT count(numOrden) as cant_comp_destino1
    FROM ${BD_STG}.tmp093168_kpi36_detafo_mdb
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/***************************GENERA DETALLE DE DIFERENCIAS ***********************/
/********************************************************************************/

  
SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok; 

CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_01} AS ( 
    SELECT 
             x0.num_docidenti,
             x0.cod_formul,
             x0.num_orden,    
             x0.num_periodo,
             x1.numRuc as num_rucB
      FROM ${BD_STG}.tmp093168_kpi36_detafo_tr x0
      FULL JOIN ${BD_STG}.tmp093168_kpi36_detafo_mdb x1 ON
      x0.num_docidenti=x1.numRuc and 
      x0.cod_formul=x1.codFormulario and 
      x0.num_orden=x1.numOrden and 
      x0.num_periodo=x1.perPeriodo
) WITH DATA NO PRIMARY INDEX;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
    SELECT 
          y0.num_docidenti,
          y0.cod_formul,
          y0.num_orden,    
          y0.num_periodo
    FROM ${BD_STG}.tmp093168_total_${KPI_01} y0
    WHERE y0.num_rucB is null
   ) WITH DATA NO PRIMARY INDEX ;

 .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_01}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
    INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
    SELECT  '${PERIODO}',
            '99',
            '${KPI_01}',
            CURRENT_DATE,
            (SELECT cant_comp_origen FROM ${BD_STG}.tmp093168_kpigr36_obs_cnorigen ),
            (SELECT cant_comp_destino1 FROM ${BD_STG}.tmp093168_kpigr36_cndestino1 ),
            case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 and 
				           (select count(*) from ${BD_STG}.tmp093168_kpi36_detafo_tr)<>0)
			then 1 else 0 end,
        	(select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}),
			(select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_docidenti is null),
			(select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_docidenti=num_rucB);
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/******************************************************************************/

    .EXPORT FILE ${FILE_KPI01};

    LOCK ROW FOR ACCESS
    SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01}
    ORDER BY 1,2,4; 

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

    .EXPORT RESET;
    
SEL CURRENT_TIMESTAMP;

/**************************************************************/

DROP TABLE ${BD_STG}.tmp093168_kpi36_detafo_tr;
DROP TABLE ${BD_STG}.tmp093168_kpi36_detafo_mdb;

DROP TABLE ${BD_STG}.tmp093168_kpigr36_obs_cnorigen;
DROP TABLE ${BD_STG}.tmp093168_kpigr36_cndestino1;

/**************************************************************/

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







#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - Staging
### $6 : Ruta Log TERADATA
### $7 : Periodo :2022
### sh /work1/teradata/shells/093168/J093168_KPIGRP14OBS.sh tdsunat usr_carga_prod twusr_carga_prod bddwedq bddwestg bddwelnd /work1/teradata/log/093168 2022
### sh /work1/teradata/shells/093168/J093168_KPIGRP14OBS.sh tdtp01s2 usr_carga_desa twusr_carga_desa bddwedqd bddwestgd bddwelndd /work1/teradata/log/093168 2022

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
KPI_01='K014032022'
KPI_02='K014042022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_CAS514_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIF_'${KPI_02}'_CAS514_FVIRVSMODB_'${DATE}'.unl'

rm -f ${FILE_KPI01}
rm -f ${FILE_KPI02}

bteq <<EOF>${FILELOG} 2>${FILEERR}

LOGON ${LOGONDB};

DATABASE ${BD_DQ};

.SET FORMAT OFF;
.SET WIDTH 32000;
.SET SEPARATOR '|';
.SET TITLEDASHES OFF;

SEL CURRENT_TIMESTAMP;

/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

------------Genera Detalle Transaccional-------------------------------


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi14_detcpeobs_tr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_tr;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;


CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_tr as
(
	SELECT 
		DISTINCT 
		TRIM(x0.num_ruc) as num_ruc,
		coalesce(x2.ind_presdj,0) as ind_presdj,
		x0.ann_ejercicio,
		TRIM(x0.num_ruc_emisor) as num_ruc_emisor,
		TRIM(x0.cod_tip_doc) as cod_tip_doc,
		TRIM(x0.ser_doc) as ser_doc,
		TRIM(x0.num_doc) as num_doc
	FROM ${BD_STG}.t8157cpgastoobserv x0
	LEFT JOIN ${BD_LND}.ddp_ruc x1 ON x0.num_ruc_emisor = x1.ddp_numruc
	LEFT JOIN ${BD_STG}.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
	WHERE x0.ann_ejercicio = '${PERIODO}' 
	AND x0.ind_tip_gasto = '05' 
	AND x0.fec_pago >= CAST('${PERIODO}-01-01' AS DATE FORMAT 'YYYY-MM-DD') 
	AND x0.fec_pago <= CAST('${PERIODO}-12-31' AS DATE FORMAT 'YYYY-MM-DD')
	AND x0.ind_inconsistencia <> 'I1'
	AND (substr(x0.num_ruc,1,1) <>'2' OR  x0.num_ruc in (select num_ruc from ${BD_STG}.tmp093168_rucs20_incluir))
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 



/***************************FVIRTUAL**************************************************/

------ Genera Detalle de COMPROBANTES OBSERVADOS en FVIRTUAL-------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi14_detcpeobs_fv';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_fv;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_fv
AS
(
	SELECT
		DISTINCT 
        extract(year from fec_comprob) as ann_ejercicio,
		TRIM(a.num_ruc) as num_ruc,
		b.ind_presdj,
		TRIM(a.num_doc_emisor) as num_doc_emisor,
		TRIM(a.cod_tip_comprob) as cod_tip_comprob ,
		TRIM(a.num_serie) as num_serie,
		TRIM(a.num_comprob)  as num_comprob
	FROM ${BD_STG}.t12734cas514det a
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
	WHERE a.cod_tip_gasto = '05'
	AND a.ind_est_archpers = '0'  -- OBSERVADO
	AND a.ind_archpers = '1'
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/************************MONGO DB*****************************************************/

------ Genera Detalle de COMPROBANTES OBSERVADOS en MONGODB-------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi14_detcpeobs_mdb';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_mdb;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_mdb
AS
(
SELECT DISTINCT 
        extract(year from fec_comprob) as ann_ejercicio,
		TRIM(a.num_ruc) as num_ruc,
		b.ind_presdj,
		TRIM(a.num_doc_emisor) as num_doc_emisor,
		TRIM(a.cod_tip_comprob) as cod_tip_comprob ,
		TRIM(a.num_serie) as num_serie,
		TRIM(a.num_comprob)  as num_comprob
FROM ${BD_STG}.t12734cas514det_mongodb a
INNER JOIN ${BD_STG}.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers = '0'  -- OBSERVADO
AND a.ind_archpers = '1'  -- personalizado
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/*************************************************************************************/
/*************************************************************************************/
/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr14_obs_cnorigen';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr14_obs_cnorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr14_obs_cnorigen AS
(
	SELECT ind_presdj,count(num_doc) as cant_comp_origen
	FROM ${BD_STG}.tmp093168_kpi14_detcpeobs_tr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

---------2. Conteo en FVirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr14_obs_cndestino1';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr14_obs_cndestino1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr14_obs_cndestino1 AS
(
	SELECT ind_presdj,count(num_comprob) as cant_comp_destino1
	FROM ${BD_STG}.tmp093168_kpi14_detcpeobs_fv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--------3 Conteo en MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr14_obs_cndestino2';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr14_obs_cndestino2;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr14_obs_cndestino2 AS
(
	SELECT ind_presdj,count(num_comprob) as cant_comp_destino2
	FROM ${BD_STG}.tmp093168_kpi14_detcpeobs_mdb
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/	

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_01} AS (
		SELECT 		x0.ann_ejercicio,
					x0.num_ruc,
					x0.num_ruc_emisor,
					x0.cod_tip_doc,
					x0.ser_doc,
					x0.num_doc,
					x1.num_ruc as num_rucB
		FROM ${BD_STG}.tmp093168_kpi14_detcpeobs_tr x0
		FULL JOIN ${BD_STG}.tmp093168_kpi14_detcpeobs_fv x1 ON 
		x0.ann_ejercicio=x1.ann_ejercicio and
		x0.num_ruc=x1.num_ruc and
		x0.num_ruc_emisor=x1.num_doc_emisor and 
		x0.cod_tip_doc=x1.cod_tip_comprob and 
		x0.ser_doc=x1.num_serie and 
		x0.num_doc=x1.num_comprob  
) WITH DATA NO PRIMARY INDEX;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

    CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
     SELECT y0.ann_ejercicio,
			y0.num_ruc as num_ruc_trab,
			y0.num_ruc_emisor,
			y0.cod_tip_doc,
			y0.ser_doc,
			y0.num_doc
	 FROM ${BD_STG}.tmp093168_total_${KPI_01} y0
	 WHERE y0.num_rucB is null
	) WITH DATA NO PRIMARY INDEX;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------------------------------------------------------------------------------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_02};
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

    CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_02} AS (
       SELECT  x0.ann_ejercicio,
			x0.num_ruc,
			x0.num_doc_emisor,
			x0.cod_tip_comprob,
			x0.num_serie,
			x0.num_comprob,
			x1.num_ruc as num_rucB
		FROM ${BD_STG}.tmp093168_kpi14_detcpeobs_fv x0
		FULL JOIN  ${BD_STG}.tmp093168_kpi14_detcpeobs_mdb x1 ON
		x0.ann_ejercicio=x1.ann_ejercicio and
		x0.num_ruc=x1.num_ruc and 
		x0.num_doc_emisor=x1.num_doc_emisor and
		x0.cod_tip_comprob=x1.cod_tip_comprob and
		x0.num_serie=x1.num_serie and
		x0.num_comprob=x1.num_comprob
	) WITH DATA NO PRIMARY INDEX;	

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_02}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_02} AS (
	SELECT 
			y0.ann_ejercicio,
			y0.num_ruc AS num_ruc_trab,
			y0.num_doc_emisor AS num_ruc_emisor,
			y0.cod_tip_comprob,
			y0.num_serie,
			y0.num_comprob	
	FROM ${BD_STG}.tmp093168_total_${KPI_02} y0
	WHERE y0.num_rucB is null
    ) WITH DATA NO PRIMARY INDEX;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;



/********************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_01}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
	SELECT 
			'${PERIODO}',
			x0.ind_presdj,
			'${KPI_01}' ,
			CURRENT_DATE,
			case when x0.ind_presdj=0 then 
			(select coalesce(sum(cant_comp_origen),0) from ${BD_STG}.tmp093168_kpigr14_obs_cnorigen) 
			else 0 end as cant_origen,
			coalesce(x1.cant_comp_destino1,0) as cant_destino,
			case when x0.ind_presdj=0 then 
            	case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 and 
				           (select count(*) from ${BD_STG}.tmp093168_kpi14_detcpeobs_tr)<>0)
				then 1 else 0 end 
        	end as ind_incuniv,
        	case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}) END as cnt_regdif,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc is null) end as cnt_regdif_do,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc=num_rucB) end as cnt_regcoinc
	FROM
	(
		select y.ind_presdj,SUM(y.cant_comp_origen) as cant_comp_origen
		from
			(
				select * from ${BD_STG}.tmp093168_kpigr14_obs_cnorigen
				union all select 1,0 from (select '1' agr1) a
				union all select 0,0 from (select '0' agr0) b
			) y group by 1
	) x0
	LEFT JOIN ${BD_STG}.tmp093168_kpigr14_obs_cndestino1 x1 
	ON  x0.ind_presdj=x1.ind_presdj
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

-------------------------------------------------------------------------------------

    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_02}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
	SELECT 
			'${PERIODO}',
			x0.ind_presdj,
			'${KPI_02}',
			CURRENT_DATE,
			x0.cant_comp_destino1 AS cant_origen,
			case when x0.ind_presdj=0  then 
				(select coalesce(sum(cant_comp_destino2),0) from ${BD_STG}.tmp093168_kpigr14_obs_cndestino2) 
			else 0 end AS cant_destino,
			case when x0.ind_presdj=0 then 
			case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02})=0 and 
					  (select count(*) from ${BD_STG}.tmp093168_kpi14_detcpeobs_fv)<>0)
			then 1 else 0 end 
			end as ind_incuniv,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02}) END as cnt_regdif,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc is null) end as cnt_regdif_do,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc=num_rucB) end as cnt_regcoinc
	FROM (
		select y.ind_presdj,SUM(y.cant_comp_destino1) as cant_comp_destino1
		from
		(
			select * from ${BD_STG}.tmp093168_kpigr14_obs_cndestino1
			union all select 1,0 from (select '1' agr1) a
			union all select 0,0 from (select '0' agr0) b
		) y group by 1
	) x0
	LEFT JOIN ${BD_STG}.tmp093168_kpigr14_obs_cndestino2 x1 
	ON x0.ind_presdj=x1.ind_presdj
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
/************************************************************************************************/
	
	.EXPORT FILE ${FILE_KPI01};
	
	LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01}
	ORDER BY num_ruc_trab,num_ruc_emisor;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.EXPORT RESET;
	
	.EXPORT FILE ${FILE_KPI02};

    LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_02}
	ORDER BY num_ruc_trab,num_ruc_emisor;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	.EXPORT RESET;

/************************************************************************************************/

SEL CURRENT_TIMESTAMP;

DROP TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_tr;
DROP TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_fv;
DROP TABLE ${BD_STG}.tmp093168_kpi14_detcpeobs_mdb;
DROP TABLE ${BD_STG}.tmp093168_kpigr14_obs_cnorigen;
DROP TABLE ${BD_STG}.tmp093168_kpigr14_obs_cndestino1;
DROP TABLE ${BD_STG}.tmp093168_kpigr14_obs_cndestino2;



    
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
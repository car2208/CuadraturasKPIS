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
##  sh /work1/teradata/shells/093168/J093168_KPIGRP01.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG /work1/teradata/log/093168 2022 2023-01-31
##  sh /work1/teradata/shells/093168/J093168_KPIGRP01.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD /work1/teradata/log/093168 2022 2023-01-31
################################################################################


if [ $# -ne 8 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
path_log_TD=${6}
PERIODO=${7}
FECHA_CORTE=${8}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K001012022'
KPI_02='K001022022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_CAS107_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIF_'${KPI_02}'_CAS107_FVIRVSMODB_'${DATE}'.unl'

per_lim="$(echo ${FECHA_CORTE}|awk -F'-' '{ print $2 $1}')"


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
/*******************Extrae recibos por honorarios******************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_cantrecibos';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_cantrecibos;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;


CREATE MULTISET TABLE ${BD_STG}.tmp093168_cantrecibos as
(
	SELECT distinct num_ruc ,num_serie ,cod_tipcomp ,num_comprob
	FROM ${BD_STG}.t3639recibo
	WHERE EXTRACT(YEAR FROM fec_emision_rec) = ${PERIODO}
	AND ind_estado_rec = '0'
	AND cod_tipcomp = '01'
	AND fec_emision_rec <= DATE '${FECHA_CORTE}'
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_cantnotascredito';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_cantnotascredito;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_cantnotascredito as
(
	SELECT distinct num_ruc ,num_serie ,'07' as cod_tipcomp ,num_nota as num_comprob
	FROM ${BD_STG}.t3634notacredito 
	WHERE EXTRACT(YEAR FROM fec_emision_nc) = ${PERIODO}
	AND ind_estado_nc = '0'
	AND cod_tipcomp_ori = '01'
	AND fec_emision_nc <= DATE '${FECHA_CORTE}'
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/*******************Última DJ******************/



SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_udjkpi1';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_udjkpi1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_udjkpi1 as
(
SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,t2.t03lltt_ruc,
         t2.t03periodo,t2.t03f_presenta 
FROM 
		(
			SELECT 
			t03periodo,
			t03lltt_ruc,
			t03formulario,
			MAX(t03f_presenta) as t03f_presenta,
			MAX(t03nresumen) as t03nresumen,
			MAX(t03norden) as t03norden 
			FROM ${BD_STG}.t03djcab
			WHERE t03formulario = '0616' 
			AND t03periodo between '${PERIODO}01' and '${PERIODO}12'
			AND t03f_presenta <= DATE '${FECHA_CORTE}'
		    GROUP BY 1,2,3
		    
		) t1
INNER JOIN ${BD_STG}.t03djcab t2 ON t2.t03periodo = t1.t03periodo 
AND t2.t03lltt_ruc = t1.t03lltt_ruc
AND t2.t03formulario = t1.t03formulario
AND t2.t03f_presenta = t1.t03f_presenta
AND t2.t03nresumen = t1.t03nresumen
AND t2.t03norden = t1.t03norden
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/*********Extrae RxHe de Form 0616 , útlima dj***********/
 


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_cantrecibosf616';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_cantrecibosf616;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_cantrecibosf616 as
(
SELECT DISTINCT x0.num_docide_dec,x0.num_serie_cp,x0.tip_cp,CAST(x0.num_cp AS INTEGER) AS num_cp
FROM ${BD_STG}.t1209f616rddet x0
INNER JOIN ${BD_STG}.tmp093168_udjkpi1 x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '${PERIODO}01' and '${PERIODO}12'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '02'
AND substr(x0.num_serie_cp,1,1) ='E'
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_cantnotcredtf616';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_cantnotcredtf616;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_cantnotcredtf616 as
(
SELECT DISTINCT x0.num_docide_dec,x0.num_serie_cp,x0.tip_cp,CAST(x0.num_cp AS INTEGER) AS num_cp
FROM ${BD_STG}.t1209f616rddet x0
INNER JOIN ${BD_STG}.tmp093168_udjkpi1 x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '${PERIODO}01' and '${PERIODO}12'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '07'
AND substr(x0.num_serie_cp,1,1) ='E'
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/******Union de RxH de CPE y Form 0616**************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_detcantrxhe';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_detcantrxhe;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_detcantrxhe as
(
	SELECT  TRIM(num_ruc) AS num_ruc,
	        TRIM(num_serie) as num_serie,
	        '02' cod_tipcomp,num_comprob 
	FROM ${BD_STG}.tmp093168_cantrecibos
	UNION
	SELECT TRIM(num_docide_dec),
		   TRIM(num_serie_cp),
		   TRIM(tip_cp),
		   num_cp 
	FROM ${BD_STG}.tmp093168_cantrecibosf616
	UNION 
	SELECT  TRIM(num_ruc) AS num_ruc,
	        TRIM(num_serie) as num_serie,
	        cod_tipcomp,
			num_comprob 
	FROM ${BD_STG}.tmp093168_cantnotascredito
	UNION
	SELECT
		   TRIM(num_docide_dec),
		   TRIM(num_serie_cp),
		   TRIM(tip_cp),
		   num_cp 
	FROM ${BD_STG}.tmp093168_cantnotcredtf616
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/


-------1. Detalle de RxHe en transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_detcantrxhetr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_detcantrxhetr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_detcantrxhetr
AS(
	SELECT DISTINCT x0.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
			x0.num_serie,x0.cod_tipcomp,x0.num_comprob
	FROM ${BD_STG}.tmp093168_detcantrxhe x0
	LEFT JOIN ${BD_STG}.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
	WHERE substr(x0.num_ruc,1,1) <>'2' or  x0.num_ruc in (select num_ruc from ${BD_STG}.tmp093168_rucs20_incluir)
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

-------2. Detalle de RxHe en Archivo Personalizado Fvirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_detcantrxhefv';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_detcantrxhefv;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_detcantrxhefv
AS(
	SELECT DISTINCT 
			x1.num_ruc,
			COALESCE(x1.ind_presdj,0) as ind_presdj,
			x0.num_serie,
			x0.tip_comp,
			x0.num_comp
	FROM ${BD_STG}.t5373cas107 x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.tip_comp in ('02','07')
	AND SUBSTR(x0.num_serie,1,1) = 'E'
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


-------3. Detalle de RxHe en Archivo Personalizado MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_detcantrxhemdb';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_detcantrxhemdb;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_detcantrxhemdb
AS(
	SELECT DISTINCT 
	        x1.num_ruc,
	        COALESCE(x1.ind_presdj,0) as ind_presdj,
			x0.num_serie,
			x0.COD_TIPCOMP,
			x0.num_comp
	FROM ${BD_STG}.T5373CAS107_MONGODB x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.COD_TIPCOMP in ('02','07')
	AND SUBSTR(x0.num_serie,1,1) = 'E'
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi01_cnorigen';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi01_cnorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi01_cnorigen AS
(
	SELECT ind_presdj,count(num_comprob) as cant_rxh_origen
	FROM ${BD_STG}.tmp093168_detcantrxhetr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------2. Conteo en FVirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi01_cndestino1';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi01_cndestino1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi01_cndestino1 AS
(
	SELECT ind_presdj,count(num_comp) as cant_rxh_destino1
	FROM ${BD_STG}.tmp093168_detcantrxhefv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--------3 Conteo en MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi02_cndestino2';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi02_cndestino2	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi02_cndestino2 AS
(
	SELECT ind_presdj,count(num_comp) as cant_rxh_destino2
	FROM ${BD_STG}.tmp093168_detcantrxhemdb
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
	SELECT x0.num_ruc,x0.num_serie,x0.cod_tipcomp,x0.num_comprob,x1.num_ruc as num_rucB  
	FROM ${BD_STG}.tmp093168_detcantrxhetr x0
	FULL JOIN ${BD_STG}.tmp093168_detcantrxhefv x1 on 
	x0.num_ruc=x1.num_ruc and
	x0.num_serie=x1.num_serie and
	x0.cod_tipcomp=x1.tip_comp and
	x0.num_comprob=cast(x1.num_comp as integer)
) WITH DATA NO PRIMARY INDEX;
	

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	
	
	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
	SELECT   DISTINCT 
				y0.num_ruc as num_ruc_trab,
				y0.num_serie,
				y0.cod_tipcomp,
				y0.num_comprob
	FROM ${BD_STG}.tmp093168_total_${KPI_01} y0
	WHERE y0.num_rucB is null
	) WITH DATA NO PRIMARY INDEX;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_02}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_02} AS (
	SELECT x0.num_ruc,x0.num_serie,x0.tip_comp,cast(x0.num_comp as integer)  as num_comprob,x1.num_ruc as num_rucB
	FROM ${BD_STG}.tmp093168_detcantrxhefv x0
	FULL JOIN ${BD_STG}.tmp093168_detcantrxhemdb x1 ON
	x0.num_ruc=x1.num_ruc and
	x0.num_serie=x1.num_serie and
	x0.tip_comp=x1.cod_tipcomp and
	cast(x0.num_comp as integer)=cast(x1.num_comp as integer) 
) WITH DATA NO PRIMARY INDEX;



SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_02};
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

    CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_02} AS (
	SELECT    DISTINCT 
		      y0.num_ruc as num_ruc_trab,
			  y0.num_serie,
			  y0.tip_comp  as cod_tipcomp,
			  y0.num_comprob
	FROM ${BD_STG}.tmp093168_total_${KPI_02} y0 
	WHERE y0.num_rucB is null
	) WITH DATA NO PRIMARY INDEX;

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
			x0.ind_presdj,
			'${KPI_01}' ,
			CURRENT_DATE,
			case when x0.ind_presdj=0 then (select coalesce(sum(cant_rxh_origen),0) from ${BD_STG}.tmp093168_kpi01_cnorigen) else 0 end as cant_origen,
			coalesce(x1.cant_rxh_destino1,0) as cant_destino,
			case when x0.ind_presdj=0 then 
			case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 and (select count(*) from ${BD_STG}.tmp093168_detcantrxhetr)<>0) then 1 else 0 end 
			end as ind_incuniv,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}) END as cnt_regdif_od,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc is null) end as cnt_regdif_do ,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc=num_rucB) end as cnt_regcoinc
	FROM 
	(
		select y.ind_presdj,SUM(y.cant_rxh_origen) as cant_rxh_origen
		from
		(
			select * from ${BD_STG}.tmp093168_kpi01_cnorigen
			union all select 1,0 from (select '1' agr1) a
			union all select 0,0 from (select '0' agr0) b
		) y group by 1
	) x0
	LEFT JOIN ${BD_STG}.tmp093168_kpi01_cndestino1 x1 
	ON  x0.ind_presdj=x1.ind_presdj
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

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
			x0.cant_rxh_destino1 AS cant_origen,
			case when x0.ind_presdj=0  then (select coalesce(sum(cant_rxh_destino2),0) from ${BD_STG}.tmp093168_kpi02_cndestino2) else 0 end AS cant_destino,
			case when x0.ind_presdj=0 then 
			case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02})=0 and (select count(*) from ${BD_STG}.tmp093168_detcantrxhefv)<>0) then 1 else 0 end 
			end as ind_incuniv,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02}) END as cnt_regdif_od,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc is null) end as cnt_regdif_do,
			case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc=num_rucB) end as cnt_regcoinc
	FROM 
	(
		select y.ind_presdj,SUM(y.cant_rxh_destino1) as cant_rxh_destino1
		from
		(
			select * from ${BD_STG}.tmp093168_kpi01_cndestino1
			union all select 1,0 from (select '1' agr1) a
			union all select 0,0 from (select '0' agr0) b
		) y group by 1
	) x0
	LEFT JOIN ${BD_STG}.tmp093168_kpi02_cndestino2 x1 
	ON x0.ind_presdj=x1.ind_presdj
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/********************************************************************************/
	
	.EXPORT FILE ${FILE_KPI01};

	LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01} 
	ORDER BY num_ruc_trab

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.EXPORT RESET;

	.EXPORT FILE ${FILE_KPI02};

    LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_02}
	ORDER BY num_ruc_trab;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	.EXPORT RESET;


/********************************************************************************/

	DROP TABLE ${BD_STG}.tmp093168_cantrecibos;
	DROP TABLE ${BD_STG}.tmp093168_cantnotascredito;
	DROP TABLE ${BD_STG}.tmp093168_udjkpi1;
	DROP TABLE ${BD_STG}.tmp093168_cantrecibosf616;
	DROP TABLE ${BD_STG}.tmp093168_cantnotcredtf616;
	DROP TABLE ${BD_STG}.tmp093168_detcantrxhe;
	DROP TABLE ${BD_STG}.tmp093168_detcantrxhetr;
	DROP TABLE ${BD_STG}.tmp093168_detcantrxhefv;
	DROP TABLE ${BD_STG}.tmp093168_detcantrxhemdb;
	DROP TABLE ${BD_STG}.tmp093168_kpi01_cnorigen;
	DROP TABLE ${BD_STG}.tmp093168_kpi01_cndestino1;
	DROP TABLE ${BD_STG}.tmp093168_kpi02_cndestino2;

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
#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - Staging
### $6 : Ruta Log TERADATA
### $7: Periodo :${PERIODO}
##  sh /work1/teradata/shells/093168/J093168_KPIGRP03.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG bddwelnd /work1/teradata/log/093168 2022 2023-01-31
##  sh /work1/teradata/shells/093168/J093168_KPIGRP03.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD bddwelndd /work1/teradata/log/093168 2022 2023-01-31

################################################################################


if [ $# -ne 10 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


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
FECHA_CORTE=${10}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K003012022'
KPI_02='K003022022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_CAS108_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIF_'${KPI_02}'_CAS108_FVIRVSMODB_'${DATE}'.unl'


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

/*******************Obtiene última dj form 0601********************************************/


/**********Obtiene Última DJ Form 0601 ********************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_udjkpigr3';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_udjkpigr3;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;


CREATE MULTISET TABLE ${BD_STG}.tmp093168_udjkpigr3 as
(
  SELECT t2.num_nabono as t03nabono,
        t2.num_orden as t03norden,
        t2.cod_formul as t03formulario,
        t2.num_ruc as  t03lltt_ruc,
        t2.cod_per as t03periodo,
        t2.fec_presenta as t03f_presenta 
  FROM 
  (
    SELECT 
			  cod_per ,
				num_ruc ,
				cod_formul ,
				MAX(fec_presenta) as fec_presenta,
				MAX(num_resumen) as num_resumen,
				MAX(num_orden) as num_orden 
    FROM ${BD_TB}.t8593djcab
    WHERE cod_formul = '0601' 
    AND cod_per BETWEEN '${PERIODO}01' and '${PERIODO}12'
    AND fec_presenta <=DATE '${FECHA_CORTE}'
    AND fec_finvig=2000101
		AND ind_deldwe='0'
    GROUP BY 1,2,3
  ) AS t1 
  INNER JOIN ${BD_TB}.t8593djcab t2 ON t2.cod_per = t1.cod_per 
AND t2.num_ruc = t1.num_ruc
AND t2.cod_formul = t1.cod_formul
AND t2.fec_presenta = t1.fec_presenta
AND t2.num_resumen = t1.num_resumen
AND t2.num_orden = t1.num_orden
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


/******************Obtiene periodos declarados en el PLAME***************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr3_periodos_compag';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr3_periodos_compag;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr3_periodos_compag AS
(

  SELECT DISTINCT
    CASE WHEN x0.cod_tip_doc_ide = '06' 
         THEN x2.ddp_numruc
    ELSE  x3.dds_numruc END AS num_rucs,
      x0.num_ruc,
      x0.num_paq as num_nabono,
      x0.formulario as cod_formul,
      x0.norden as num_orden,
      x0.per_decla
  FROM ${BD_STG}.t4583com_pag x0 
  INNER JOIN ${BD_STG}.tmp093168_udjkpigr3 x1 
  ON  x1.t03lltt_ruc = x0.num_ruc
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  LEFT JOIN ${BD_LND}.ddp_ruc x2 
  ON x0.num_doc_ide=x2.ddp_numruc AND x0.cod_tip_doc_ide='06'
  LEFT JOIN ${BD_LND}.dds_ruc x3 
  ON x0.num_doc_ide=x3.dds_nrodoc AND cast(cast(x0.cod_tip_doc_ide AS int) as varchar(2))=x3.dds_docide
  WHERE x0.per_decla BETWEEN '${PERIODO}01' AND '${PERIODO}12' 
  AND x0.formulario = '0601'
  AND x0.ind_com_pag = 'D'
  AND x0.mto_servicio > 0
  AND  num_rucs IS NOT NULL

) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


/**************************Obtiene Última DJ de Form 0616 ********************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_udj_f616_kpigr3';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_udj_f616_kpigr3;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_udj_f616_kpigr3 as
(
  SELECT t2.num_nabono as t03nabono,
        t2.num_orden as t03norden,
        t2.cod_formul as t03formulario,
        t2.num_ruc as  t03lltt_ruc,
         t2.cod_per as t03periodo,
         t2.fec_presenta as t03f_presenta  
  FROM 
  (
  SELECT cod_per ,
				num_ruc ,
				cod_formul ,
				MAX(fec_presenta) as fec_presenta,
				MAX(num_resumen) as num_resumen,
				MAX(num_orden) as num_orden 
  FROM ${BD_TB}.t8593djcab
  WHERE cod_formul = '0616' 
  AND cod_per BETWEEN '${PERIODO}01' and '${PERIODO}12'
  AND fec_presenta <=DATE '${FECHA_CORTE}'
  AND fec_finvig=2000101
	AND ind_deldwe='0'
  GROUP BY 1,2,3
  ) AS t1 
  INNER JOIN ${BD_TB}.t8593djcab t2 ON t2.cod_per = t1.cod_per 
AND t2.num_ruc = t1.num_ruc
AND t2.cod_formul = t1.cod_formul
AND t2.fec_presenta = t1.fec_presenta
AND t2.num_resumen = t1.num_resumen
AND t2.num_orden = t1.num_orden
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


/*************** Obtiene periodos declarados en F0616****************************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr3_periodos_f0616';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr3_periodos_f0616;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr3_periodos_f0616 as
(
  SELECT  DISTINCT x0.num_docide_dec,
          x0.num_docide_ret,
          x0.num_paq as num_nabono,
          x0.formulario as cod_formul,
          x0.norden as num_orden,
          x0.per_periodo 
  FROM ${BD_STG}.t1209f616rddet x0, ${BD_STG}.tmp093168_udj_f616_kpigr3 x1
  WHERE x0.tip_docide_dec = '6'
  AND x0.per_periodo between '${PERIODO}01' and '${PERIODO}12'
  AND x0.formulario = '0616'
  AND x0.tip_cp = '99'
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  AND LENGTH(TRIM(x0.num_docide_ret)) = '11'
) WITH DATA NO PRIMARY INDEX;
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


/**************Union de Periodos Plame con Periodos f0616*********************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr03_detcnt_tr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcnt_tr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr03_detcnt_tr AS
(
  
 SELECT  TRIM(num_docide_dec) as num_ruc,
         TRIM(num_docide_ret) as num_docide_empl,
         num_nabono,
         cod_formul,
         num_orden,
         per_periodo as per_decla
  FROM ${BD_STG}.tmp093168_kpigr3_periodos_f0616
  UNION
  SELECT 
         TRIM(num_rucs),
         TRIM(num_ruc) as num_docide_empl,
         num_nabono,
         cod_formul,
         num_orden,
         per_decla
  FROM ${BD_STG}.tmp093168_kpigr3_periodos_compag
)WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/

-------1. Detalle de Periodos  en transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr03_detcntpertr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcntpertr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr03_detcntpertr
AS(
SELECT 
    DISTINCT x0.num_ruc,
    COALESCE(x1.ind_presdj,0) as ind_presdj,
        x0.num_docide_empl,
        x0.num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_decla
FROM ${BD_STG}.tmp093168_kpigr03_detcnt_tr x0
LEFT JOIN ${BD_STG}.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
INNER JOIN ${BD_LND}.dds_ruc x2 ON x0.num_ruc=x2.dds_numruc 
WHERE x2.dds_domici = '1'  AND x2.dds_docide IN ('1','2','3','4','5','7','8')
AND (substr(x0.num_ruc,1,1) <>'2' OR  x0.num_ruc in (select num_ruc from ${BD_STG}.tmp093168_rucs20_incluir))
) WITH DATA NO PRIMARY INDEX ;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


-------2. Detalle de Periodos en Archivo Personalizado Fvirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr03_detcntperfv';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcntperfv;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr03_detcntperfv
AS
(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc,
          x0.periodo
  FROM ${BD_STG}.T5376CAS108 x0
  INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  --WHERE x0.tip_doc='06'
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


-------3. Detalle de Periodos en Archivo Personalizado MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr03_detcntpermdb';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcntpermdb;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr03_detcntpermdb
AS(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc, x0.num_perservicio
  FROM ${BD_STG}.T5376CAS108_MONGODB x0
  INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  --WHERE x0.COD_TIPDOC='06'
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr03_cnorigen';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr03_cnorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr03_cnorigen AS
(
  SELECT y.ind_presdj,count(y.per_decla) as cant_per_origen
  FROM (
    SELECT 
    DISTINCT num_ruc,ind_presdj,num_docide_empl,per_decla
    FROM ${BD_STG}.tmp093168_kpigr03_detcntpertr
  ) y
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

---------2. Conteo en FVirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr03_cndestino1';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr03_cndestino1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr03_cndestino1 AS
(
  SELECT ind_presdj,count(periodo) as cant_per_destino1
  FROM ${BD_STG}.tmp093168_kpigr03_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


--------3 Conteo en MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr03_cndestino2';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr03_cndestino2 ;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr03_cndestino2 AS
(
  SELECT ind_presdj,count(num_perservicio) as cant_per_destino2
  FROM ${BD_STG}.tmp093168_kpigr03_detcntpermdb
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
    SELECT  x0.num_ruc,x0.num_docide_empl,x1.periodo,x1.num_ruc as num_rucB
      FROM (
    SELECT 
    DISTINCT num_ruc,num_docide_empl,SUBSTR(z.per_decla,5,2)||SUBSTR(z.per_decla,1,4) as per_decla
    FROM ${BD_STG}.tmp093168_kpigr03_detcntpertr z
      ) x0
      FULL JOIN ${BD_STG}.tmp093168_kpigr03_detcntperfv x1 
      ON
      x0.num_ruc=x1.num_ruc and
	x0.num_docide_empl=x1.num_doc and
	x0.per_decla =x1.periodo
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;	

    CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
      SELECT y0.num_ruc,y0.num_docide_empl,y0.periodo
	    FROM ${BD_STG}.tmp093168_total_${KPI_01} y0
	    WHERE y0.num_rucB is null
    ) WITH DATA NO PRIMARY INDEX;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

---------------------------------------------------------------------------------------------  

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_02}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_02} AS (
	SELECT x0.num_ruc,x0.num_doc,x0.periodo,x1.num_ruc as num_rucB 
	FROM ${BD_STG}.tmp093168_kpigr03_detcntperfv x0
	FULL JOIN ${BD_STG}.tmp093168_kpigr03_detcntpermdb x1 ON
	x0.num_ruc=x1.num_ruc and
	x0.num_doc=x1.num_doc and
	x0.periodo=x1.num_perservicio
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_02};
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

  CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_02} AS (
   SELECT y0.num_ruc as num_ruc_trab,
			   y0.num_doc as num_ruc_empl,
			   y0.periodo as per_dif
		FROM ${BD_STG}.tmp093168_total_${KPI_02} y0 
		WHERE y0.num_rucB is null
  ) WITH DATA NO PRIMARY INDEX;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell; 



/********************INSERT EN TABLA FINAL***********************************/

  DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
  WHERE COD_KPI='${KPI_01}' AND FEC_CARGA=CURRENT_DATE;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
  SELECT
          '${PERIODO}',
        x0.ind_presdj,
        '${KPI_01}' ,
        CURRENT_DATE,
          case when x0.ind_presdj=0 then 
                  (select coalesce(sum(cant_per_origen),0) from ${BD_STG}.tmp093168_kpigr03_cnorigen) 
        else 0 end as cant_origen,
        coalesce(x1.cant_per_destino1,0) as cant_destino,
        case when x0.ind_presdj=0 then 
        case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 and
                  (select count(*) from ${BD_STG}.tmp093168_kpigr03_detcntpertr)<>0)
        then 1 else 0 end end as ind_incuniv,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}) END as cnt_regdif,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc is null) end as cnt_regdif_do,
			  case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc=num_rucB) end as cnt_regcoinc
  FROM 
  (
      select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
      from
      (
        select * from ${BD_STG}.tmp093168_kpigr03_cnorigen
        union all select 1,0 from (select '1' agr1) a
        union all select 0,0 from (select '0' agr0) b
      ) y group by 1
  )  x0
  LEFT JOIN ${BD_STG}.tmp093168_kpigr03_cndestino1 x1 
  ON x0.ind_presdj=x1.ind_presdj
  ;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
  WHERE COD_KPI='${KPI_02}' AND FEC_CARGA=CURRENT_DATE;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
  SELECT '${PERIODO}',
          x0.ind_presdj,
          '${KPI_02}',
          CURRENT_DATE,
          x0.cant_per_destino1 AS cant_origen,
          case when x0.ind_presdj=0  then 
                  (select coalesce(sum(cant_per_destino2),0) from ${BD_STG}.tmp093168_kpigr03_cndestino2)
          else 0 end AS cant_destino,
        case when x0.ind_presdj=0 then 
        case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02})=0 and
                  (select count(*) from ${BD_STG}.tmp093168_kpigr03_detcntperfv)<>0)
        then 1 else 0 end end as ind_incuniv,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02}) END as cnt_regdif,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc is null) end as cnt_regdif_do,
			  case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc=num_rucB) end as cnt_regcoinc
  FROM 
  (
    select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
      from
      (
        select * from ${BD_STG}.tmp093168_kpigr03_cndestino1
        union all select 1,0 from (select '1' agr1) a
        union all select 0,0 from (select '0' agr0) b
      ) y group by 1
  ) x0
  LEFT JOIN ${BD_STG}.tmp093168_kpigr03_cndestino2 x1 
  ON x0.ind_presdj=x1.ind_presdj
  ;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;
 
/********************************************************************************/


 

 	.EXPORT FILE ${FILE_KPI01};
   
   LOCK ROW FOR ACCESS
	 SELECT 
	    DISTINCT 
	        y0.num_ruc as num_ruc_trab,
	        y0.num_docide_empl as num_ruc_empl,
	        y0.num_nabono,
	        y0.cod_formul,
	        y0.num_orden,
	        y0.per_decla as per_dif
	  FROM ${BD_STG}.tmp093168_kpigr03_detcntpertr y0
	  INNER JOIN ${BD_STG}.tmp093168_dif_${KPI_01} y1 
	  ON  y0.num_ruc=y1.num_ruc 
	  AND y0.num_docide_empl=y1.num_docide_empl
	  AND SUBSTR(y0.per_decla,5,2)||SUBSTR(y0.per_decla,1,4)=y1.periodo
    ORDER BY y0.num_ruc,y0.per_decla ;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;. 

	.EXPORT RESET;

   	.EXPORT FILE ${FILE_KPI02};
    
    LOCK ROW FOR ACCESS
	  SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_02}
    ORDER BY 1,3;
    
	 .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	.EXPORT RESET;

/********************************************************************************/

DROP TABLE ${BD_STG}.tmp093168_udjkpigr3;
DROP TABLE ${BD_STG}.tmp093168_kpigr3_periodos_compag;
DROP TABLE ${BD_STG}.tmp093168_udj_f616_kpigr3;

DROP TABLE ${BD_STG}.tmp093168_kpigr3_periodos_f0616;
DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcnt_tr;

DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcntpertr;
DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcntperfv;
DROP TABLE ${BD_STG}.tmp093168_kpigr03_detcntpermdb;


DROP TABLE ${BD_STG}.tmp093168_kpigr03_cnorigen;
DROP TABLE ${BD_STG}.tmp093168_kpigr03_cndestino1;
DROP TABLE ${BD_STG}.tmp093168_kpigr03_cndestino2 ;

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
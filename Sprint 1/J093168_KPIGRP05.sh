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
##  sh /work1/teradata/shells/093168/J093168_KPIGRP05.sh tdsunat usr_carga_prod twusr_carga_prod BDDWETB BDDWEDQ BDDWESTG BDDWELND /work1/teradata/log/093168 2022 2023-01-31
##  sh /work1/teradata/shells/093168/J093168_KPIGRP05.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWETBD BDDWEDQD BDDWESTGD BDDWELNDD /work1/teradata/log/093168 2022 2023-01-31

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
KPI_01='K005012022'
KPI_02='K005022022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_CAS111_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIF_'${KPI_02}'_CAS111_FVIRVSMODB_'${DATE}'.unl'


rm -f ${FILE_KPI01}
rm -f ${FILE_KPI02}

let UNO=1
let NPER=$PERIODO+$UNO


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
/************Obtiene úlima DJ ***********************************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_udjkpigr5';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_udjkpigr5;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_udjkpigr5 as
(
  SELECT t2.num_nabono as t03nabono,
         t2.num_orden as t03norden,
         t2.cod_formul as t03formulario,
         t2.num_ruc as  t03lltt_ruc,
         t2.cod_per as t03periodo,
         t2.fec_presenta as t03f_presenta 
  FROM 
  (
  SELECT  cod_per ,
          num_ruc ,
          cod_formul ,
          MAX(fec_presenta) as fec_presenta,
          MAX(num_resumen) as num_resumen,
          MAX(num_orden) as num_orden 
  FROM ${BD_TB}.t8593djcab
  WHERE cod_formul = '0601' 
  AND cod_per IN ( '${NPER}01','${NPER}02')
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


SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr5_periodos_ctaind';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr5_periodos_ctaind;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr5_periodos_ctaind as
(
  SELECT  DISTINCT 
        x2.dds_numruc as num_docide_aseg,
        x0.num_docide_empl,
        x0.num_paquete as num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM ${BD_STG}.t727nctaind x0
INNER JOIN  ${BD_STG}.tmp093168_udjkpigr5 x1 
ON  x1.t03lltt_ruc = x0.num_docide_empl
AND x1.t03nabono = x0.num_paquete
AND x1.t03formulario = x0.cod_formul 
AND x1.t03norden = x0.num_orden
INNER JOIN  ${BD_LND}.dds_ruc x2
ON x0.num_docide_aseg=x2.dds_nrodoc 
AND cast(cast(x0.tip_docide_aseg as int) as varchar(3))=x2.dds_docide
WHERE x0.per_aporta ='${PERIODO}13'
AND x0.cod_formul = '0601'
AND x0.cod_tributo = '030502'
AND x0.ind_exist_aseg IN ('6','8')
AND x0.tip_trabajador NOT IN ('23','24','26','35')
AND x0.mto_base_imp IS NOT NULL
AND x0.mto_base_imp > 0  
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------------Listado de Periodos de aportacion ---------------------------------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr05_detcnt_tr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcnt_tr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr05_detcnt_tr AS
(
   SELECT
        num_docide_aseg as num_ruc,
        num_docide_empl,
        num_nabono,
        cod_formul,
        num_orden,
        per_aporta
   FROM ${BD_STG}.tmp093168_kpigr5_periodos_ctaind
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/

-------1. Detalle de Periodos  en transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr05_detcntpertr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcntpertr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr05_detcntpertr
AS(
SELECT 
    DISTINCT x0.num_ruc,
    COALESCE(x1.ind_presdj,0) as ind_presdj,
        x0.num_docide_empl,
        x0.num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM ${BD_STG}.tmp093168_kpigr05_detcnt_tr x0
LEFT JOIN ${BD_STG}.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
WHERE substr(x0.num_ruc,1,1) <>'2' or  x0.num_ruc in (select num_ruc from ${BD_STG}.tmp093168_rucs20_incluir)
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

-------2. Detalle de Periodos en Archivo Personalizado Fvirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr05_detcntperfv';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcntperfv;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr05_detcntperfv
AS(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc,
          x0.periodo
  FROM ${BD_STG}.t5377cas111 x0
  INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  WHERE x0.tip_doc = '06'
  AND x0.periodo='13${PERIODO}'
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

-------3. Detalle de Periodos en Archivo Personalizado MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr05_detcntpermdb';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcntpermdb;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr05_detcntpermdb
AS(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc, x0.num_perservicio
  FROM ${BD_STG}.T5377CAS111_MONGODB x0
  INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  WHERE x0.COD_TIPDOC ='06'
  AND x0.num_perservicio='13${PERIODO}'
) WITH DATA NO PRIMARY INDEX; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/**************************************************************************************/
/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr05_cnorigen';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr05_cnorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr05_cnorigen AS
(
  SELECT y.ind_presdj,count(y.per_aporta) as cant_per_origen
  FROM
  (
    SELECT DISTINCT num_ruc,ind_presdj,num_docide_empl,per_aporta
    FROM ${BD_STG}.tmp093168_kpigr05_detcntpertr
  ) y
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------2. Conteo en FVirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr05_cndestino1';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr05_cndestino1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr05_cndestino1 AS
(
  SELECT ind_presdj,count(periodo) as cant_per_destino1
  FROM ${BD_STG}.tmp093168_kpigr05_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

--------3 Conteo en MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr05_cndestino2';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr05_cndestino2;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr05_cndestino2 AS
(
  SELECT ind_presdj,count(num_perservicio) as cant_per_destino2
  FROM ${BD_STG}.tmp093168_kpigr05_detcntpermdb
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
    SELECT x0.num_ruc,x0.num_docide_empl,x1.periodo,x1.num_ruc as num_rucB
      FROM (  
        SELECT DISTINCT num_ruc,num_docide_empl,SUBSTR(z.per_aporta,5,2)||SUBSTR(z.per_aporta,1,4) as per_aporta
        FROM ${BD_STG}.tmp093168_kpigr05_detcntpertr z
      ) x0
      FULL JOIN ${BD_STG}.tmp093168_kpigr05_detcntperfv x1 
      ON
      x0.num_ruc=x1.num_ruc and
	    x0.num_docide_empl=x1.num_doc and
	    x0.per_aporta=x1.periodo
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

 -----------------------------------------------------------------------------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_total_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_total_${KPI_02}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_total_${KPI_02} AS (
	SELECT x0.num_ruc,x0.num_doc,x0.periodo,x1.num_ruc as num_rucB 
	FROM ${BD_STG}.tmp093168_kpigr05_detcntperfv x0
	FULL JOIN ${BD_STG}.tmp093168_kpigr05_detcntpermdb x1 ON
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
 )  WITH DATA NO PRIMARY INDEX;

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
        '${KPI_01}',
          CURRENT_DATE,
          case when x0.ind_presdj=0 then (select coalesce(sum(cant_per_origen),0) from ${BD_STG}.tmp093168_kpigr05_cnorigen) else 0 end as cant_origen,
          coalesce(x1.cant_per_destino1,0) as cant_destino,
          case when x0.ind_presdj=0 then 
        case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 and
                   (select count(*) from ${BD_STG}.tmp093168_kpigr05_detcntpertr)<>0)
        then 1 else 0 end 
        end as ind_incuniv,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}) END as cnt_regdif,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc is null) end as cnt_regdif_do,
			  case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_01} where num_ruc=num_rucB) end as cnt_regcoinc
        
  FROM 
  (
      select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
      from
      (
        select * from ${BD_STG}.tmp093168_kpigr05_cnorigen
        union all select 1,0 from (select '1' agr1) a
        union all select 0,0 from (select '0' agr0) b
      ) y group by 1
  ) x0
  LEFT JOIN ${BD_STG}.tmp093168_kpigr05_cndestino1 x1 
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
        case when x0.ind_presdj=0  then (select coalesce(sum(cant_per_destino2),0) from ${BD_STG}.tmp093168_kpigr05_cndestino2) else 0 end AS cant_destino,
        case when x0.ind_presdj=0 then 
        case when ((select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02})=0 and
                  (select count(*) from ${BD_STG}.tmp093168_kpigr05_detcntperfv)<>0)
        then 1 else 0 end end as ind_incuniv,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02}) END as cnt_regdif,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc is null) end as cnt_regdif_do,
			  case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_total_${KPI_02} where num_ruc=num_rucB) end as cnt_regcoinc
  FROM 
  (
      select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
      from
      (
        select * from ${BD_STG}.tmp093168_kpigr05_cndestino1
        union all select 1,0 from (select '1' agr1) a
        union all select 0,0 from (select '0' agr0) b
      ) y group by 1
  ) x0
  LEFT JOIN ${BD_STG}.tmp093168_kpigr05_cndestino2 x1 
  ON x0.ind_presdj=x1.ind_presdj
  ;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;


/********************************************************************************/

  .EXPORT FILE ${FILE_KPI01};

    LOCK ROW FOR ACCESS
    SELECT 
          y0.num_ruc as num_ruc_trab,
          y0.num_docide_empl as num_ruc_empl,
          y0.num_nabono,
          y0.cod_formul,
          y0.num_orden,
          y0.per_aporta as per_dif
    FROM ${BD_STG}.tmp093168_kpigr05_detcntpertr y0
    INNER JOIN  ${BD_STG}.tmp093168_dif_${KPI_01} y1 
    ON y0.num_ruc=y1.num_ruc 
    AND y0.num_docide_empl=y1.num_docide_empl
    AND SUBSTR(y0.per_aporta,5,2)||SUBSTR(y0.per_aporta,1,4)=y1.periodo
    ORDER BY y0.num_ruc,y0.per_aporta
    ;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell;
    
    .EXPORT RESET;

    
    .EXPORT FILE ${FILE_KPI02};

    LOCK ROW FOR ACCESS
    SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_02}
    ORDER BY 1,3; 

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

    .EXPORT RESET;

/********************************************************************************/


DROP TABLE ${BD_STG}.tmp093168_udjkpigr5;
DROP TABLE ${BD_STG}.tmp093168_kpigr5_periodos_ctaind;
DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcnt_tr;

DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcntpertr;
DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcntperfv;
DROP TABLE ${BD_STG}.tmp093168_kpigr05_detcntpermdb;

DROP TABLE ${BD_STG}.tmp093168_kpigr05_cnorigen;
DROP TABLE ${BD_STG}.tmp093168_kpigr05_cndestino1;
DROP TABLE ${BD_STG}.tmp093168_kpigr05_cndestino2;


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
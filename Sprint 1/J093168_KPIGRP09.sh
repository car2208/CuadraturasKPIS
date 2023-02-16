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
##  sh /work1/teradata/shells/093168/J093168_KPIGRP09.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG /work1/teradata/log/093168 2022 2023-02-06
##  sh /work1/teradata/shells/093168/J093168_KPIGRP09.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD /work1/teradata/log/093168 2022 2023-02-06

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
KPI_01='K009012022'
KPI_02='K009022022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_CAS130_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIF_'${KPI_02}'_CAS130_FVIRVSMODB_'${DATE}'.unl'


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

/**********Obtiene Última DJ Form 0601 ********************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_udjkpigr9';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_udjkpigr9;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_udjkpigr9 as
(
  SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,
         t2.t03lltt_ruc,t2.t03periodo,t2.t03f_presenta 
  FROM 
  (
    SELECT t03periodo,
           t03lltt_ruc,
           t03formulario,
           MAX(t03f_presenta) as t03f_presenta,
           MAX(t03nresumen) as t03nresumen,
           MAX(t03norden)  as t03norden
    FROM ${BD_STG}.t03djcab
    WHERE t03formulario = '0601' 
    AND t03periodo BETWEEN '${PERIODO}01' and '${PERIODO}12'
    AND t03f_presenta <=DATE '${FECHA_CORTE}'
    GROUP BY 1,2,3
  ) AS t1 
  INNER JOIN ${BD_STG}.t03djcab t2 ON t2.t03periodo = t1.t03periodo 
  AND t2.t03lltt_ruc = t1.t03lltt_ruc
  AND t2.t03formulario = t1.t03formulario
  AND t2.t03f_presenta = t1.t03f_presenta
  AND t2.t03nresumen = t1.t03nresumen
  AND t2.t03norden = t1.t03norden
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr9_periodos_compag';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr9_periodos_compag;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr9_periodos_compag AS
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
  INNER JOIN ${BD_STG}.tmp093168_udjkpigr9 x1 
  ON  x1.t03lltt_ruc = x0.num_ruc
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  LEFT JOIN ${BD_STG}.ddp x2 
  ON x0.num_doc_ide=x2.ddp_numruc AND x0.cod_tip_doc_ide='06'
  LEFT JOIN ${BD_STG}.dds x3 
  ON x0.num_doc_ide=x3.dds_nrodoc AND cast(cast(x0.cod_tip_doc_ide AS int) as varchar(2))=x3.dds_docide
  WHERE x0.per_decla BETWEEN '${PERIODO}01' AND '${PERIODO}12' 
  AND x0.formulario = '0601'
  AND x0.ind_com_pag = 'D'
  AND x0.mto_retenido > 0
  AND  num_rucs IS NOT NULL
  ) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


/***********************************************************************************************************/
--------------Genera tablas detalles de las fuentes---------------------------------------------------------

------------1. Detalle transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_detcntpertr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntpertr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_detcntpertr
AS(
SELECT DISTINCT x0.num_rucs AS num_ruc_trab,
      x0.num_ruc as num_ruc_empl,
      x0.per_decla,
      x0.cod_formul,
      x0.num_orden,
      coalesce(x1.ind_presdj,0) as ind_presdj
FROM ${BD_STG}.tmp093168_kpigr9_periodos_compag x0
LEFT JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_rucs = x1.num_ruc
WHERE substr(x0.num_rucs,1,1) <>'2' or  x0.num_rucs in (select num_ruc from ${BD_STG}.tmp093168_rucs20_incluir)
) WITH DATA NO PRIMARY INDEX ;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

------------2. Detalle T1851

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_detcntper1851';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntper1851;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_detcntper1851
AS(
SELECT  DISTINCT x0.num_ruc,x0.NUM_RUC_RET,x0.per_tri,x0.cod_for,x0.num_ord,
        coalesce(x1.ind_presdj,0) as ind_presdj
FROM ${BD_STG}.t1851ret_rta x0
 INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_ruc = x1.num_ruc
WHERE  SUBSTR(x0.per_tri,1,4) = '2022'
   AND x0.cod_tri = '030402'
   AND x0.cod_for IN ('0621','0601')
   AND x0.mto_ret > 0
) WITH DATA NO PRIMARY INDEX ;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

------------3. Detalle FVIRTUAL

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_detcntperfv';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntperfv;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_detcntperfv
AS(
SELECT DISTINCT x1.num_ruc,x0.num_doc,x0.per_mes,x0.num_formul,x0.num_ord,
        coalesce(x1.ind_presdj,0) as ind_presdj
FROM ${BD_STG}.t12735cas130 x0
 INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
WHERE x0.cod_tip_doc = '06'
AND x0.mto_retenido > 0
) WITH DATA NO PRIMARY INDEX ;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


------------4. Detalle MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_detcntpermdb';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntpermdb;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_detcntpermdb
AS(
SELECT DISTINCT x1.num_ruc,x0.num_doc,x0.num_perimpreten,x0.cod_formul,x0.num_numorden,
    coalesce(x1.ind_presdj,0) as ind_presdj
FROM ${BD_STG}.t12735cas130_mongodb x0
 INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
WHERE x0.cod_tipdoc = '06'
AND x0.num_mtoretenido > 0
) WITH DATA NO PRIMARY INDEX ;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_cnorigen';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_cnorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_cnorigen AS
(
  SELECT ind_presdj,count(per_decla) as cant_per_origen
  FROM ${BD_STG}.tmp093168_kpigr09_detcntpertr
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

---------2. Conteo en T1851

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_cnorigent1851';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_cnorigent1851;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_cnorigent1851 AS
(
  SELECT ind_presdj,count(per_tri) as cant_per_origent1851
  FROM ${BD_STG}.tmp093168_kpigr09_detcntper1851
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------3. Conteo en FVirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_cndestino1';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_cndestino1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_cndestino1 AS
(
  SELECT ind_presdj,count(per_mes) as cant_per_destino1
  FROM ${BD_STG}.tmp093168_kpigr09_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


--------4. Conteo en MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr09_cndestino2';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr09_cndestino2 ;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr09_cndestino2 AS
(
  SELECT ind_presdj,count(num_perimpreten) as cant_per_destino2
  FROM ${BD_STG}.tmp093168_kpigr09_detcntpermdb
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/********************INSERT EN TABLA FINAL***********************************/

  DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
  WHERE COD_KPI='${KPI_01}' AND FEC_CARGA=CURRENT_DATE;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '${PERIODO}',
          z.ind_presdj,
         '${KPI_01}',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
             case when x0.ind_presdj=0 then 
                      (select coalesce(sum(cant_per_origen),0) from ${BD_STG}.tmp093168_kpigr09_cnorigen) 
            else 0 end as cant_origen,
             coalesce(x1.cant_per_origent1851,0) as cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
          from
          (
            select * from ${BD_STG}.tmp093168_kpigr09_cnorigen
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      )  x0
      LEFT JOIN ${BD_STG}.tmp093168_kpigr09_cnorigent1851 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
  WHERE COD_KPI='${KPI_02}' AND FEC_CARGA=CURRENT_DATE;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '${PERIODO}',
          z.ind_presdj,
          '${KPI_02}',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             case when x0.ind_presdj=0  then 
                      (select coalesce(sum(cant_per_destino2),0) from ${BD_STG}.tmp093168_kpigr09_cndestino2)
             else 0 end AS cant_destino
      FROM 
      (
        select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
          from
          (
            select * from ${BD_STG}.tmp093168_kpigr09_cndestino1
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN ${BD_STG}.tmp093168_kpigr09_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  /*******************************************************************************************/

  /********************************************************************************/
/*************TRANSACCIONAL MENOS T1851*********************************/
  
  
SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
    SELECT 
          y0.num_ruc_trab,
          y0.num_ruc_empl,
          y0.per_decla,
          y0.cod_formul,
          y0.num_orden
    FROM
    (
      SELECT
            num_ruc_trab,
            num_ruc_empl,
            per_decla,
            cod_formul,
            num_orden
      FROM ${BD_STG}.tmp093168_kpigr09_detcntpertr
      EXCEPT ALL
      SELECT
            NUM_RUC,
            NUM_RUC_RET,
            PER_TRI,
            COD_FOR,
            NUM_ORD
      FROM ${BD_STG}.tmp093168_kpigr09_detcntper1851
     ) y0
   ) WITH DATA PRIMARY INDEX (num_ruc_trab,per_decla);

 .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

  /*******************FVIRTUAL MENOS MONGO*********************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_02}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	

  CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_02} AS (
  SELECT 
      y0.num_ruc as num_ruc_trab,
        y0.num_doc as num_ruc_empl,
        y0.per_mes,
        y0.num_formul,
        y0.num_ord
  FROM
  (
      SELECT
            num_ruc,
            TRIM(num_doc) as num_doc,
            per_mes,
            num_formul,
            num_ord
      FROM  ${BD_STG}.tmp093168_kpigr09_detcntperfv     
      EXCEPT ALL
      SELECT 
            num_ruc,
            TRIM(NUM_DOC),
            NUM_PERIMPRETEN,
            COD_FORMUL,
            NUM_NUMORDEN
      FROM ${BD_STG}.tmp093168_kpigr09_detcntpermdb
  )  y0
  ) WITH DATA PRIMARY INDEX (num_ruc_trab,per_mes);

  .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

  .EXPORT FILE ${FILE_KPI01};

  LOCK ROW FOR ACCESS
  SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01}  
  ORDER BY num_ruc_trab,per_decla;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

  .EXPORT RESET;

  .EXPORT FILE ${FILE_KPI02};

  LOCK ROW FOR ACCESS
  SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_02}
  ORDER BY num_ruc_trab,per_mes;
  
  .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

  .EXPORT RESET;

/*******************************************************************************************/

    DROP TABLE ${BD_STG}.tmp093168_udjkpigr9;
    DROP TABLE ${BD_STG}.tmp093168_kpigr9_periodos_compag;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntpertr;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntper1851;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntperfv;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_detcntpermdb;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_cnorigen;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_cnorigent1851;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_cndestino1;
    DROP TABLE ${BD_STG}.tmp093168_kpigr09_cndestino2 ;

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
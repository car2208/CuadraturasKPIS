#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - Staging
### $6 : Ruta Log TERADATA
### $7: Periodo :2022

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
KPI_01='K002012022'
KPI_02='K002022022'
FILE_KPI01='/work1/teradata/dat/093168/DIFF_'${KPI_01}'_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIFF_'${KPI_02}'_'${DATE}'.unl'



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

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_udjkpigr2';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_udjkpigr2;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_udjkpigr2 as
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
AND t03f_presenta <= DATE '${FECHA_CORTE}'
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


/*******************Obtiene periodos declarados en el PLAME******************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr2_periodos_ctaind';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr2_periodos_ctaind;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr2_periodos_ctaind as
(
SELECT  DISTINCT 
        x2.dds_numruc as num_docide_aseg,
        x0.num_docide_empl,
        x0.num_paquete as num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM ${BD_STG}.t727nctaind x0
INNER JOIN  ${BD_STG}.tmp093168_udjkpigr2 x1 
ON  x1.t03lltt_ruc = x0.num_docide_empl
AND x1.t03nabono = x0.num_paquete
AND x1.t03formulario = x0.cod_formul 
AND x1.t03norden = x0.num_orden
INNER JOIN  ${BD_STG}.dds x2
ON x0.num_docide_aseg=x2.dds_nrodoc AND cast(cast(x0.tip_docide_aseg as int) as varchar(3))=x2.dds_docide
WHERE x0.per_aporta BETWEEN '${PERIODO}01' and '${PERIODO}12'
AND x0.cod_formul = '0601'
AND x0.cod_tributo = '030402'
AND x0.tip_trabajador = '67'
AND x0.mto_base_imp IS NOT NULL
AND x0.mto_aporta IS NOT NULL
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*********************************************************************************************/
---------Obtiene periodos declarados en el PLAME otros Ingresos-------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr2_periodos_compag';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr2_periodos_compag;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr2_periodos_compag AS
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
INNER JOIN ${BD_STG}.tmp093168_udjkpigr2 x1 
ON  x1.t03lltt_ruc = x0.num_ruc
AND x1.t03nabono = x0.num_paq
AND x1.t03formulario = x0.formulario 
AND x1.t03norden = x0.norden
LEFT JOIN ${BD_STG}.ddp x2 
ON x0.num_doc_ide=x2.ddp_numruc AND x0.cod_tip_doc_ide='06'
LEFT JOIN ${BD_STG}.dds x3 
ON x0.num_doc_ide=x3.dds_nrodoc AND cast(cast(x0.cod_tip_doc_ide AS int) as varchar(3))=x3.dds_docide
WHERE x0.per_decla BETWEEN '${PERIODO}01' and '${PERIODO}12'
AND x0.formulario = '0601'
AND x0.ind_com_pag = 'O'
AND x0.mto_servicio IS NOT NULL
AND  num_rucs IS NOT NULL
) WITH DATA NO PRIMARY INDEX;
   
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/**********************************************************************************************************/
-----------------Union de universos de periodos declarados en form 0601 Cuenta Individual y Otros Ingresos

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr02_detcnt_tr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr02_detcnt_tr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr02_detcnt_tr AS
(
   SELECT
        TRIM(num_docide_aseg) as num_ruc,
        TRIM(num_docide_empl) as num_docide_empl,
        num_nabono,
        cod_formul,
        num_orden,
        per_aporta
   FROM ${BD_STG}.tmp093168_kpigr2_periodos_ctaind
   UNION
   SELECT TRIM(num_rucs),
    	  TRIM(num_ruc),
    	  num_nabono,
    	  cod_formul,
    	  num_orden,
    	  per_decla
   FROM ${BD_STG}.tmp093168_kpigr2_periodos_compag
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/

-------1. Detalle de Periodos  en transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr02_detcntpertr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr02_detcntpertr;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr02_detcntpertr
AS(
SELECT 
		DISTINCT x0.num_ruc,
		COALESCE(x1.ind_presdj,0) as ind_presdj,
        x0.num_docide_empl,
        x0.num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM ${BD_STG}.tmp093168_kpigr02_detcnt_tr x0
LEFT JOIN ${BD_STG}.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

-------2. Detalle de Periodos en Archivo Personalizado Fvirtual


DROP TABLE ${BD_STG}.tmp093168_kpigr02_detcntperfv;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr02_detcntperfv
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_doc,
					x0.periodo
	FROM ${BD_STG}.t5373cas107 x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.tip_comp = ' '
	AND x0.num_serie = ' '
	AND x0.num_comp = ' '
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

-------3. Detalle de Periodos en Archivo Personalizado MongoDB

DROP TABLE ${BD_STG}.tmp093168_kpigr02_detcntpermdb;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr02_detcntpermdb
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_doc, x0.num_perservicio
	FROM ${BD_STG}.T5373CAS107_MONGODB x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.COD_TIPCOMP = ' '
	AND x0.num_serie = ' '
	AND x0.num_comp = ' '
) WITH DATA NO PRIMARY INDEX ; 

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE ${BD_STG}.tmp093168_kpigr02_cnorigen;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr02_cnorigen AS
(
	SELECT ind_presdj,count(per_aporta) as cant_per_origen
	FROM ${BD_STG}.tmp093168_kpigr02_detcntpertr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

---------2. Conteo en FVirtual
DROP TABLE ${BD_STG}.tmp093168_kpigr02_cndestino1;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr02_cndestino1 AS
(
	SELECT ind_presdj,count(periodo) as cant_per_destino1
	FROM ${BD_STG}.tmp093168_kpigr02_detcntperfv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

--------3 Conteo en MongoDB

DROP TABLE ${BD_STG}.tmp093168_kpigr02_cndestino2	;
CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr02_cndestino2 AS
(
	SELECT ind_presdj,count(num_perservicio) as cant_per_destino2
	FROM ${BD_STG}.tmp093168_kpigr02_detcntpermdb
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
			       x0.cant_per_origen as cant_origen,
			       coalesce(x1.cant_per_destino1,0) as cant_destino
			FROM ${BD_STG}.tmp093168_kpigr02_cnorigen x0
			LEFT JOIN ${BD_STG}.tmp093168_kpigr02_cndestino1 x1 
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
			       coalesce(x1.cant_per_destino2,0) AS cant_destino
			FROM ${BD_STG}.tmp093168_kpigr02_cndestino1 x0
			LEFT JOIN ${BD_STG}.tmp093168_kpigr02_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;


	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/	
	
	.EXPORT FILE ${FILE_KPI01};

	LOCK ROW FOR ACCESS
	SELECT DISTINCT 
		'${KPI_01}' as cod_kpi,
	    y0.num_ruc,
		y0.ind_presdj,
        y0.num_docide_empl,
        y0.num_nabono,
        y0.cod_formul,
        y0.num_orden,
        y0.per_aporta
	FROM ${BD_STG}.tmp093168_kpigr02_detcntpertr y0
	INNER JOIN
	(
		SELECT DISTINCT num_ruc,ind_presdj,num_docide_empl,
		                SUBSTR(per_aporta,5,2)||SUBSTR(per_aporta,1,4) as per_aporta
		FROM ${BD_STG}.tmp093168_kpigr02_detcntpertr
		EXCEPT ALL
		SELECT num_ruc,ind_presdj,num_doc,periodo 
		FROM ${BD_STG}.tmp093168_kpigr02_detcntperfv
	) y1 
	ON y0.num_ruc=y1.num_ruc 
	AND y0.ind_presdj=y1.ind_presdj 
	AND y0.num_docide_empl=y1.num_docide_empl
	AND SUBSTR(y0.per_aporta,5,2)||SUBSTR(y0.per_aporta,1,4)=y1.per_aporta;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;. 

	.EXPORT RESET;


	.EXPORT FILE ${FILE_KPI02};
    
    LOCK ROW FOR ACCESS
	SELECT   DISTINCT '${KPI_02}'as cod_kpi,
		      y0.num_ruc,
		      y0.ind_presdj,
			  y0.num_doc,
			  y0.periodo  
	FROM
	(
		SELECT num_ruc,ind_presdj,num_doc,periodo 
		FROM ${BD_STG}.tmp093168_kpigr02_detcntperfv
		EXCEPT ALL
		SELECT num_ruc,ind_presdj,num_doc,num_perservicio
		FROM ${BD_STG}.tmp093168_kpigr02_detcntpermdb
	) y0;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	.EXPORT RESET;

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
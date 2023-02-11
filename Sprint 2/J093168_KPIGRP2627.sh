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
### sh /work1/teradata/shells/093168/J093168_KPIGRP2627.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG /work1/teradata/log/093168 2022
### sh /work1/teradata/shells/093168/J093168_KPIGRP2627.sh tdtp01s2 usr_carga_desa twusr_carga_desa bddwedqd bddwestgd /work1/teradata/log/093168 2022

################################################################################


if [ $# -ne 7 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
path_log_TD=${6}
PERIODO=${7}


MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_03='K026012022'
KPI_04='K026022022'
KPI_01='K027012022'
KPI_02='K027022022'
FILE_KPI01='/work1/teradata/dat/093168/DIF_'${KPI_01}'_CAS100_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIF_'${KPI_02}'_CAS100_FVIRVSMODB_'${DATE}'.unl'
FILE_KPI03='/work1/teradata/dat/093168/DIF_'${KPI_01}'_CAS100_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI04='/work1/teradata/dat/093168/DIF_'${KPI_02}'_CAS100_FVIRVSMODB_'${DATE}'.unl'


rm -f ${FILE_KPI01}
rm -f ${FILE_KPI02}
rm -f ${FILE_KPI03}
rm -f ${FILE_KPI04}

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

------------Genera Detalle Transaccional Comprobantes Válidos-------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi26_detcpeval_tr';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_tr;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_tr
AS
(
SELECT
	DISTINCT 
	coalesce(x2.ind_presdj,0) as ind_presdj,
	TRIM(x0.num_ruc) as num_ruc,
	x0.per_pag as per_pago,
	x0.num_formul,
	x0.num_doc as num_ordope,
	x0.MTO_BASIMP as mto_gravado
FROM ${BD_STG}.t7910pagorta x0 
LEFT JOIN ${BD_STG}.ddp x1 ON x0.num_ruc = x1.ddp_numruc
LEFT JOIN ${BD_STG}.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
WHERE x0.per_pag between '${PERIODO}01' and '${PERIODO}12'
AND x0.ind_tippag = '3'

)
WITH DATA NO PRIMARY INDEX;


.IF ERRORCODE <> 0 THEN .GOTO error_shell; 



/***************************FVIRTUAL**************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en FVIRTUAL----------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi26_detcpeval_fv';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_fv;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_fv
AS
(
SELECT 
        DISTINCT 
		TRIM(b.num_ruc) as num_ruc,
		b.ind_presdj,
		SUBSTR(a.per_pago,3,4)||SUBSTR(a.per_pago,1,2) as per_pago,
		cast(a.num_formul as smallint) num_formul,
		cast(a.NUM_ORDOPE as integer) num_ordope,
		a.MTO_GRAVADO 
FROM ${BD_STG}.T7993CAS100DET a
INNER JOIN ${BD_STG}.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
) WITH DATA NO PRIMARY INDEX;


.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/************************MONGO DB*****************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en MONGODB----------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpi26_detcpeval_mdb';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_mdb;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_mdb
AS
(
SELECT
		DISTINCT 
		TRIM(b.num_ruc) as num_ruc,
		b.ind_presdj,
		SUBSTR(a.per_pago,3,4)||SUBSTR(a.per_pago,1,2) as per_pago,
		cast(a.num_formul as smallint) num_formul,
		cast(a.NUM_ORDOPE as integer) num_ordope,
		a.MTO_GRAVADO 
FROM ${BD_STG}.T7993CAS100DET_MONGODB a
INNER JOIN ${BD_STG}.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell

/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr26_val_cnorigen';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr26_val_cnorigen;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr26_val_cnorigen AS
(
	SELECT ind_presdj,count(num_ordope) as cant_comp_origen, sum(mto_gravado) as mto_origen
	FROM ${BD_STG}.tmp093168_kpi26_detcpeval_tr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------2. Conteo en FVirtual

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr26_val_cndestino1';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr26_val_cndestino1;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr26_val_cndestino1 AS
(
	SELECT ind_presdj,count(NUM_ORDOPE) as cant_comp_destino1, sum(MTO_GRAVADO) as mto_destino1
	FROM ${BD_STG}.tmp093168_kpi26_detcpeval_fv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


--------3 Conteo en MongoDB

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr26_val_cndestino2';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpigr26_val_cndestino2	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr26_val_cndestino2 AS
(
	SELECT ind_presdj,count(NUM_ORDOPE) as cant_comp_destino2, sum(MTO_GRAVADO) as mto_destino2
	FROM ${BD_STG}.tmp093168_kpi26_detcpeval_mdb
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 



/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_01}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT '${PERIODO}',z.ind_presdj,
	       '${KPI_01}' ,
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT
			       x0.ind_presdj,
				   case when x0.ind_presdj=0 then (select coalesce(sum(cant_comp_origen),0) from ${BD_STG}.tmp093168_kpigr26_val_cnorigen) else 0 end as cant_origen,
			       coalesce(x1.cant_comp_destino1,0) as cant_destino
			FROM
			(
				select y.ind_presdj,SUM(y.cant_comp_origen) as cant_comp_origen
				from
					(
						select * from ${BD_STG}.tmp093168_kpigr26_val_cnorigen
						union all select 1,0,0 from (select '1' agr1) a
						union all select 0,0,0 from (select '0' agr0) b
					) y group by 1
			) x0
			LEFT JOIN ${BD_STG}.tmp093168_kpigr26_val_cndestino1 x1 
			ON x0.ind_presdj=x1.ind_presdj

		) z
	GROUP BY 1,2,3,4
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_02}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  '${PERIODO}',z.ind_presdj,
	        '${KPI_02}',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT x0.ind_presdj,
			       x0.cant_comp_destino1 AS cant_origen,
				   case when x0.ind_presdj=0  then (select coalesce(sum(cant_comp_destino2),0) from ${BD_STG}.tmp093168_kpigr26_val_cndestino2) else 0 end AS cant_destino
			FROM
			(
				select y.ind_presdj,SUM(y.cant_comp_destino1) as cant_comp_destino1
				from
					(
						select * from ${BD_STG}.tmp093168_kpigr26_val_cndestino1
						union all select 1,0,0 from (select '1' agr1) a
						union all select 0,0,0 from (select '0' agr0) b
					) y group by 1
			) x0
			LEFT JOIN ${BD_STG}.tmp093168_kpigr26_val_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/***********************************************27******************************************************************************************************/	

	DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_03}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO)
	SELECT '${PERIODO}',z.ind_presdj,
	       '${KPI_03}' ,
	        CURRENT_DATE,
	        SUM(z.mto_origen),
	        SUM(z.mto_destino)
	FROM
		(
			SELECT
			       x0.ind_presdj,
				   case when x0.ind_presdj=0 then (select coalesce(sum(mto_origen),0) from ${BD_STG}.tmp093168_kpigr26_val_cnorigen) else 0 end as mto_origen,
			       coalesce(x1.cant_comp_destino1,0) as mto_destino
			FROM
			(
				select y.ind_presdj,SUM(y.mto_origen) as mto_origen
					from
					(
						select * from ${BD_STG}.tmp093168_kpigr26_val_cnorigen
						union all select 1,0,0 from (select '1' agr1) a
						union all select 0,0,0 from (select '0' agr0) b
					) y group by 1

			) x0
			LEFT JOIN ${BD_STG}.tmp093168_kpigr26_val_cndestino1 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

    DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
    WHERE COD_KPI='${KPI_04}'  AND FEC_CARGA=CURRENT_DATE;
    
    .IF ERRORCODE <> 0 THEN .GOTO error_shell; 
    
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO)
	SELECT  '${PERIODO}',z.ind_presdj,
	        '${KPI_04}',
	        CURRENT_DATE,
	        SUM(z.mto_origen),
	        SUM(z.mto_destino)
	FROM
		(
			SELECT x0.ind_presdj,
			       x0.mto_destino1 AS mto_origen,
				   case when x0.ind_presdj=0  then (select coalesce(sum(mto_destino2),0) from ${BD_STG}.tmp093168_kpigr26_val_cndestino2) else 0 end AS mto_destino
			FROM 
			(
				select y.ind_presdj,SUM(y.mto_destino1) as mto_destino1
					from
					(
						select * from ${BD_STG}.tmp093168_kpigr26_val_cndestino1
						union all select 1,0,0 from (select '1' agr1) a
						union all select 0,0,0 from (select '0' agr0) b
					) y group by 1
			) x0
			LEFT JOIN ${BD_STG}.tmp093168_kpigr26_val_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;


/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/	

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_01}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

    CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
     SELECT DISTINCT 
					y0.num_ruc,
					y0.per_pago,
					y0.num_formul,
					y0.num_ordope
	FROM (
		SELECT 		num_ruc,
					per_pago,
					num_formul,
					num_ordope
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_tr
		EXCEPT ALL
		SELECT  num_ruc,
					per_pago,
					num_formul,
					num_ordope
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_fv
	) y0
	) WITH DATA NO PRIMARY INDEX;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.EXPORT FILE ${FILE_KPI01};

	LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01} 
	ORDER BY num_ruc,per_pago;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.EXPORT RESET;

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_02}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_02} AS (
	SELECT DISTINCT 
			y0.num_ruc,
					y0.per_pago,
					y0.num_formul,
					y0.num_ordope
	FROM (
	    SELECT  num_ruc,
					per_pago,
					num_formul,
					num_ordope
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_fv
		EXCEPT ALL
		SELECT   num_ruc,
					per_pago,
					num_formul,
					num_ordope
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_mdb
	) y0
    ) WITH DATA NO PRIMARY INDEX;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	.EXPORT FILE ${FILE_KPI02};

    LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_02}
	ORDER BY num_ruc,per_pago;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	.EXPORT RESET;

/********************************************************************************/
--------------------------------------PARA EL 26----------------------------------

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_03}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_03}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

    CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_03} AS (
     SELECT DISTINCT 
					y0.num_ruc,
					y0.per_pago,
					y0.num_formul,
					y0.num_ordope,
					y0.mto_gravado
	FROM (
		SELECT 		num_ruc,
					per_pago,
					num_formul,
					num_ordope,
					mto_gravado
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_tr
		EXCEPT ALL
		SELECT  	num_ruc,
					per_pago,
					num_formul,
					num_ordope,
					mto_gravado
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_fv
	) y0
	) WITH DATA NO PRIMARY INDEX;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.EXPORT FILE ${FILE_KPI03};

	LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_03} 
	ORDER BY num_ruc,per_pago;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.EXPORT RESET;

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_04}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_04}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_04} AS (
	SELECT DISTINCT 
			y0.num_ruc,
					y0.per_pago,
					y0.num_formul,
					y0.num_ordope,
					y0.mto_gravado
	FROM (
	    SELECT  num_ruc,
					per_pago,
					num_formul,
					num_ordope,
					mto_gravado
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_fv
		EXCEPT ALL
		SELECT   num_ruc,
					per_pago,
					num_formul,
					num_ordope,
					mto_gravado
		FROM ${BD_STG}.tmp093168_kpi26_detcpeval_mdb
	) y0
    ) WITH DATA NO PRIMARY INDEX;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	.EXPORT FILE ${FILE_KPI04};

    LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_04}
	ORDER BY num_ruc,per_pago;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	.EXPORT RESET;


/*********************************************************************************/

SEL CURRENT_TIMESTAMP;

DROP TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_tr;
DROP TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_fv;
DROP TABLE ${BD_STG}.tmp093168_kpi26_detcpeval_mdb;

DROP TABLE ${BD_STG}.tmp093168_kpigr26_val_cnorigen;
DROP TABLE ${BD_STG}.tmp093168_kpigr26_val_cndestino1;
DROP TABLE ${BD_STG}.tmp093168_kpigr26_val_cndestino2	;

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
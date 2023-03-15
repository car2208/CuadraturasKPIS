#### Se ejecuta desde jobs DataStage   v01/02/2023
#### ---------------------------------------------------------------------------
#### KPI06 Casilla 127 � Pagos directos de Cuarta Categor�a Pagos directos
#### Cuadratura : Cantidad de periodos declarados
#### Documento de cuadratura : V16 
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - Staging
### $6 : Ruta Log TERADATA
### $7 : Periodo :2022
### $8 : Fecha Pago Maxima : 31/01/2022
### sh /work1/teradata/shells/093168/J093168_KPIGRP06.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD /work1/teradata/log/093168 2022 2023-01-31
### sh /work1/teradata/shells/093168/J093168_KPIGRP06.sh TDSUNAT usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG /work1/teradata/log/093168 2022 2023-01-31
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
FCH_PAGO=${8}

NOM_SCRIPT='J093168_KPIGRP06.sh'

CADENA_PERIODO="'${PERIODO}01','${PERIODO}02','${PERIODO}03','${PERIODO}04','${PERIODO}05','${PERIODO}06','${PERIODO}07','${PERIODO}08','${PERIODO}09','${PERIODO}10','${PERIODO}11','${PERIODO}12'"

BD_LND='BDDWELND'

TBL_CRT=${BD_STG}'.CRT'   
TBL_HSF=${BD_STG}'.HSF'
TBL_DBT=${BD_STG}'.DBT'
TBL_DOC=${BD_STG}'.DOC'
TBL_DB2=${BD_STG}'.DB2'
TBL_DDP=${BD_STG}'.DDP_DEPEN'
TBL_T03=${BD_STG}'.T03DJCAB_DEPEN'
TBL_T04=${BD_STG}'.T04DJDET_DEPEN'
TBL_DEVOL=${BD_STG}'.devoluciones'
TBL_T869=${BD_STG}'.t869rei_cab'
TBL_T1651=${BD_STG}'.t1651sol_comp'
TBL_T3386=${BD_STG}'.t3386doc_deu_com'
TBL_CAB_PRE_RES=${BD_STG}'.cab_pre_res'
TBL_T1374=${BD_STG}'.T1374pag_rta'
TBL_T5847=${BD_STG}'.t5847ctldecl'
TBL_T5409=${BD_STG}'.T5409CAS127'
TBL_T5409_MDB=${BD_STG}'.T5409CAS127_MONGODB'
TBL_DETALLEKPI=${BD_DQ}'.T11908DETKPITRIBINT'

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K006012022'
KPI_02='K006022022'
PATH_SALIDA_PLANOS='/work1/teradata/dat/093168'

FILE_KPI01=${PATH_SALIDA_PLANOS}'/DIF_'${KPI_01}'_CAS127_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02=${PATH_SALIDA_PLANOS}'/DIF_'${KPI_02}'_CAS127_FVIRVSMODB_'${DATE}'.unl'


rm -f ${FILE_01}
rm -f ${FILE_02}

### INICIA PROCESO TERADATA
bteq <<EOF>${FILELOG} 2>${FILEERR}

LOGON ${LOGONDB};

DATABASE ${BD_DQ};

.SET FORMAT OFF;
.SET WIDTH 32000;
.SET SEPARATOR '|';
.SET TITLEDASHES OFF;

SEL CURRENT_TIMESTAMP;

/* ---- INICIO PASO1 EXTRAE PAGOS SIRAT 
/******************************* PRICO *********************************/
/**** SIRAT PRICO ***/
		
		-- PAGO DIRECTO DE 4TA
			
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_10';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_10;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_origen_10
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_10 as   
	(
		
		SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
		  FROM ${TBL_CRT}
		WHERE crt_perpag IN (${CADENA_PERIODO})
			 AND crt_codtri = '030401'
			 AND crt_indaju = '0'
			 AND crt_indpag IN (1,5)
			 AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
			 AND crt_estado <> '02'
		UNION
		SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
		  FROM ${TBL_CRT}
		WHERE crt_perpag IN (${CADENA_PERIODO})
			 AND crt_codtri = '030401'                                      
			 AND crt_tiptra = '2962'
			 AND crt_indaju = '1'
			 AND crt_indpag IN (1,5)
			 AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
			 AND crt_estado <> '02'

	) WITH DATA NO PRIMARY INDEX;
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	-- FIN PAGO DIRECTO 4TA
	-- PAGO BOLETAS - no se considera boletas en proceso de compensacion

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_1651';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_1651;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_1651
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_1651 as    
	(
		SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, b.num_doc_ori numdoc  
		from ${BD_STG}.TMP_KPI06_SIRATPRICO_10 a, ${TBL_T1651} b
		WHERE a.numruc = b.num_ruc
		AND a.formul = b.cod_for_ori
		AND a.norden = b.num_doc_ori 
		AND b.ind_con_com IN ('3','4','5')
		AND b.cod_eta_sol IN ('01','02','03')
		AND b.cod_tri ='030401'
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_09';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_09;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_origen_09
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_09 as    
	(

		SELECT  a.numruc numruc, a.perpag perpag, a.formul, a.norden  
		from ${BD_STG}.TMP_KPI06_SIRATPRICO_10 a 
		LEFT JOIN ${BD_STG}.TMP_KPI06_SIRATPRICO_1651 b ON a.numruc = b.numruc 
			AND a.formul = b.formul
			AND a.norden = b.numdoc
		WHERE b.numruc is null
		AND b.formul is null
		AND b.numdoc is null 

		
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	--- Pago Boletas - No se considera boletas en proceso de devoluci�n:
	
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_tdevo';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_tdevo;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_devo
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_tdevo as    
	(
	
		SELECT b.num_ruc numruc, b.cod_for_aso formul, b.num_doc_aso norden
		FROM ${BD_STG}.TMP_KPI06_SIRATPRICO_09 a, ${TBL_DEVOL} b
		WHERE a.numruc = b.num_ruc
		AND a.formul = b.cod_for_aso
		AND a.norden = b.num_doc_aso 
		AND b.cod_tip_sol = '02'
		AND b.ind_est_dev IN ('0','3')
		AND b.ind_res_dev IN ('0','F')
	
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_08';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_08;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_origen_08
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_08 as    
	(
	
		SELECT a.numruc, a.perpag, a.formul, a.norden 
		FROM ${BD_STG}.TMP_KPI06_SIRATPRICO_09 a 
		LEFT JOIN ${BD_STG}.TMP_KPI06_SIRATPRICO_tdevo b 
			ON a.numruc = b.numruc 
			AND a.formul = b.formul 
			AND a.norden = b.norden
		WHERE b.numruc  IS NULL
		AND b.formul IS NULL
		AND b.norden IS NULL

	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	
	--- Pago Boletas � No se considera boletas en proceso de reimputaci�n:

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_t869';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_t869;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_869
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_t869 as    
	(
	
		SELECT b.num_ruc numruc, b.cod_for formul, b.num_doc norden
		FROM ${BD_STG}.TMP_KPI06_SIRATPRICO_08 a, ${TBL_T869} b
		WHERE a.numruc = b.num_ruc
		AND a.formul = b.cod_for
		AND a.norden = b.num_doc 
		AND b.cod_for_rei = '4715'
		AND b.ind_aplica = '0'
		AND b.ind_motivo NOT IN ('0','9')
	
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_01';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_01;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_origen_01
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_01 as    
	(
	
		SELECT a.numruc, a.perpag ,a.formul ,a.norden 
		FROM ${BD_STG}.TMP_KPI06_SIRATPRICO_08 a LEFT JOIN ${BD_STG}.TMP_KPI06_SIRATPRICO_t869 b 
		ON a.numruc = b.numruc AND a.formul = b.formul AND a.norden = b.norden
		WHERE b.numruc  IS NULL
		AND b.formul IS NULL
		AND b.norden IS NULL
	
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 	
	
	
	--Saldo a favor aplicado (hsf):
		
		INSERT INTO ${BD_STG}.TMP_KPI06_SIRATPRICO_01
		SELECT hsf_numruc numruc, hsf_perpag perpag,hsf_formul,hsf_numdoc
		FROM ${TBL_HSF}
		WHERE hsf_perpag IN (${CADENA_PERIODO})
		AND hsf_codtri = '030401'
		AND hsf_tiptra = '1041'
		AND hsf_tipcta = '01'
		AND hsf_fecsaf <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD');
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	

	--  Otros cr�ditos de ley (dbt/doc):

		INSERT INTO ${BD_STG}.TMP_KPI06_SIRATPRICO_01
		SELECT a.dbt_numruc numruc, a.dbt_perpag perpag,
               a.dbt_formul,
               cast(a.dbt_numdoc as integer) as dbt_numdoc 
		FROM ${TBL_DBT} a,${TBL_DOC} b
		WHERE a.dbt_perpag IN (${CADENA_PERIODO})
		AND dbt_codtri = '030401'
		AND dbt_tiptra = 1011
		AND dbt_indrec = 0
		AND dbt_fecdoc <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
		AND doc_formul = dbt_formul
		AND doc_numdoc = dbt_numdoc
		AND doc_codcas = 347
		AND doc_valdec > 0;
			
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
			
	-- Compensaciones
	
		INSERT INTO ${BD_STG}.TMP_KPI06_SIRATPRICO_01
		SELECT  dbt_numruc numruc, dbt_perpag perpag,
                db2_formul,
                cast(db2_numdoc as integer) as db2_numdoc
		FROM ${TBL_DB2}, ${TBL_DBT}
		WHERE dbt_perpag IN (${CADENA_PERIODO})
		AND dbt_codtri = '030401'
		AND dbt_tiptra = 1011
		AND dbt_fecdoc <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
		AND dbt_formul = db2_formul
		AND dbt_numdoc = db2_numdoc
		AND dbt_codtri = db2_codtri
		AND db2_compen > 0
		UNION
		SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
		FROM ${TBL_CRT}
		WHERE crt_perpag IN (${CADENA_PERIODO})
		AND crt_codtri = '030401'  
		AND crt_tiptra = '1272'
		AND crt_indaju = '1'
		AND crt_imptri > 0
		AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD');
			
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	-- BLOQUE05

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO_06';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_06;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_origen_06
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_06 as    
	(	
		SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc 
		FROM ${TBL_T3386} a, ${TBL_CAB_PRE_RES} b
		WHERE a.cod_tri_deu = '030401'
		AND a.num_pre_res = b.num_pre_res
		AND b.ind_est_pre = '1'
		AND b.ind_eta_pre = '2'
		AND a.ind_tip_deu = '01'
		AND a.cod_tip_cal IN ('023001', '023002')
	) WITH DATA NO PRIMARY INDEX; 			  
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 		


		INSERT INTO ${BD_STG}.TMP_KPI06_SIRATPRICO_01
		SELECT a.num_ruc numruc, per_tri_des perpag,a.cod_for,a.nro_orden
		FROM ${TBL_T1651} a,${BD_STG}.TMP_KPI06_SIRATPRICO_06 b
		WHERE a.cod_for = '1648'
		AND a.nro_orden = b.numdoc 
		AND a.num_ruc = b.numruc 
		AND per_tri_des IN (${CADENA_PERIODO});

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


	-- depura duplicados
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATPRICO';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_origen_02
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO as    
	(	

		SELECT DISTINCT numruc, perpag,formul ,norden 
		 FROM ${BD_STG}.TMP_KPI06_SIRATPRICO_01
	) WITH DATA NO PRIMARY INDEX; 

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	

/******************************* MEPECO SIRAT *********************************/
/******************************* MEPECO SIRAT *********************************/
/******************************* MEPECO SIRAT *********************************/

		-- Pago DDJJ 0616

	--t_origen_03
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_T03_0'; 
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_T03_0;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_T03_0
	AS(

		SELECT DISTINCT t03lltt_ruc numruc, t03periodo perpag, t03nabono nabono, t03formulario formul, t03norden norden 
		FROM ${TBL_T03} 
		WHERE t03periodo IN (${CADENA_PERIODO})
		AND t03formulario IN ('0616','0116')	
		AND t03rechazado = '0'
		AND t03f_presenta <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')

	) WITH DATA NO PRIMARY INDEX ; 
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	
	--t_origen_04
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_T03';  
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_T03;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_T03
	AS(

    	SELECT DISTINCT numruc, perpag, CAST(formul AS SMALLINT) formul, norden
    	FROM ${TBL_T04}, ${BD_STG}.TMP_KPI06_T03_0
    	WHERE t04nabono = nabono
    	AND t04formulario = formul
    	AND t04norden = norden
    	AND t04casilla = '355'
    	AND t04i_valor IS NOT NULL
    	AND t04i_valor*1 > 0
			
	) WITH DATA NO PRIMARY INDEX ; 
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;


-- Pago Boletas 
	--t_origen_10
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO_10';  
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_10;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_10
	AS(

			SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
			FROM ${TBL_CRT}
			WHERE crt_perpag IN (${CADENA_PERIODO})
			AND crt_codtri = '030401' 
			AND crt_formul NOT IN (116,616,1083,1683)
			AND crt_indaju = '0'
			AND crt_indpag IN (1,5)
			AND crt_estado <> '02'
			AND crt_imptri > 0
			AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
				 
	) WITH DATA NO PRIMARY INDEX ; 
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;


-- Pago Boletas � No se considera boletas en proceso de compensaci�n:

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO_1651';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_1651;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_1651 as    --t_1651
	(
		SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, 
  		  CAST(b.num_doc_ori AS INTEGER) numdoc  
		from ${BD_STG}.TMP_KPI06_SIRATMEPECO_10 a, ${TBL_T1651} b
		WHERE a.numruc=b.num_ruc  
		AND b.ind_con_com IN ('3','4','5')
		AND b.cod_eta_sol IN ('01','02','03')
		AND b.cod_tri ='030401'
		
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 					



	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO_09';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_09;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_origen_09
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_09 as    
	(

		SELECT a.numruc, a.perpag, a.formul, a.numdoc 
		FROM ${BD_STG}.TMP_KPI06_SIRATMEPECO_10 a
		LEFT JOIN ${BD_STG}.TMP_KPI06_SIRATMEPECO_1651 b
		ON a.numruc=b.numruc  and a.formul=b.formul and a.numdoc= b.numdoc
		WHERE b.numruc  IS NULL
		AND b.formul IS NULL
		AND b.numdoc IS NULL
		
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 		


-- Pago Boletas � No se considera boletas en proceso de devoluci�n:


	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO_tdev'; 
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_tdev;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_dev
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_tdev as    
	(
		SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_aso formul, b.num_doc_aso numdoc
		from ${BD_STG}.TMP_KPI06_SIRATMEPECO_09 a, ${TBL_DEVOL} b
		WHERE a.numruc=b.num_ruc  
		AND a.formul = b.cod_for_aso
		AND a.numdoc = b.num_doc_aso 
		AND b.cod_tip_sol = '02'
		AND b.ind_est_dev IN ('0','3')
		AND b.ind_res_dev IN ('0','F')
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	
	--t_origen_08
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO_08'; 
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_08;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_08 as    
	(			
	
		SELECT a.numruc, a.perpag, a.formul, a.numdoc 
		FROM ${BD_STG}.TMP_KPI06_SIRATMEPECO_09 a
		LEFT JOIN ${BD_STG}.TMP_KPI06_SIRATMEPECO_tdev b
		ON a.numruc=b.numruc  and a.formul=b.formul  and a.numdoc= b.numdoc
		WHERE b.numruc IS NULL
		AND b.formul IS NULL
		AND b.numdoc IS NULL
		
	) WITH DATA NO PRIMARY INDEX; 	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 		


-- Pago Boletas � No se considera boletas en proceso de reimputaci�n:

	--t_869
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO_t869'; 
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_t869;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_t869 as    
	(				

		SELECT b.num_ruc numruc, b.cod_for formul, b.num_doc norden 
		FROM ${BD_STG}.TMP_KPI06_SIRATMEPECO_08 a, ${TBL_T869} b
		WHERE a.numruc = b.num_ruc
		AND a.formul = b.cod_for
		AND a.numdoc = b.num_doc 
		AND b.cod_for_rei = '4715'
		AND b.ind_aplica = '0'
		AND b.ind_motivo NOT IN ('0','9')
	
	) WITH DATA NO PRIMARY INDEX; 	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;		

    INSERT INTO ${BD_STG}.TMP_KPI06_T03
	SELECT a.numruc, a.perpag ,a.formul, a.numdoc
    FROM ${BD_STG}.TMP_KPI06_SIRATMEPECO_08 a 
	LEFT JOIN ${BD_STG}.TMP_KPI06_SIRATMEPECO_t869 b 
			ON a.numruc = b.numruc 
			AND a.formul = b.formul
			AND a.numdoc = b.norden
	WHERE b.numruc  IS NULL
	AND b.formul IS NULL
	AND b.norden IS NULL;
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;		

-- Bloque 03

	INSERT INTO ${BD_STG}.TMP_KPI06_T03
	SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
	FROM ${TBL_CRT}
	WHERE crt_perpag IN (${CADENA_PERIODO})
	AND crt_codtri = '030401' 
	AND crt_indaju = '1'
	AND crt_tiptra = '1472'
	AND crt_imptri > 0
	AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD');
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;				

-- Bloque 04
	--t_origen_06
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO_06'; 
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_06;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_06 as    
	(				

		SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc
		FROM ${TBL_T3386} a, ${TBL_CAB_PRE_RES} b
		WHERE a.cod_tri_deu = '030401'
		AND a.num_pre_res = b.num_pre_res
		AND b.ind_est_pre = '1'
		AND b.ind_eta_pre = '2'
		AND a.ind_tip_deu = '01'
		AND a.cod_tip_cal IN ('023001', '023002')

	) WITH DATA NO PRIMARY INDEX; 	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;		


	INSERT INTO ${BD_STG}.TMP_KPI06_T03
	SELECT  a.num_ruc numruc, a.per_tri_des perpag,a.cod_for,a.nro_orden
	FROM ${TBL_T1651} a, ${BD_STG}.TMP_KPI06_SIRATMEPECO_06 b
	WHERE a.cod_for = '1648'
	AND a.nro_orden = b.numdoc 
	AND a.num_ruc = b.numruc 
	AND a.per_tri_des IN (${CADENA_PERIODO});

	----  RESUMEN SIRATMEPECO

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI06_SIRATMEPECO';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	--t_origen_05
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO as    
	(	
		SELECT DISTINCT numruc,perpag,formul ,norden
		 FROM ${BD_STG}.TMP_KPI06_T03		
	) WITH DATA NO PRIMARY INDEX; 

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 			

/***********************************************************************************************************************/	
/***********************************************************************************************************************/
-------------------------PAGOS DIRECTOS EN TRANSACCIONAL PRICO Y MEPECO--------------------------------------------------

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr06_detcntpertr';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.tmp093168_kpigr06_detcntpertr;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr06_detcntpertr  
	AS(
        SELECT
         x0.numruc,x0.perpag as periodo,x0.formul,x0.norden,
         coalesce(x1.ind_presdj,0) as ind_presdj
		FROM(
            SELECT a.numruc, a.perpag,a.formul,a.norden 
    		FROM ${BD_STG}.TMP_KPI06_SIRATPRICO a , ${TBL_DDP} b 
    		WHERE a.numruc=b.ddp_numruc 
    		UNION 
    		SELECT a.numruc,a.perpag,a.formul ,a.norden 
    		FROM ${BD_STG}.TMP_KPI06_SIRATMEPECO a , ${TBL_DDP} b 
    		WHERE a.numruc=b.ddp_numruc
          ) x0
          INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.numruc = x1.num_ruc		
	) WITH DATA NO PRIMARY INDEX ; 			

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


--------------------------PAGOS DIRECTOS  EN  FVIRTUAL------------------------------------------------------

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr06_detcntperfv';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.tmp093168_kpigr06_detcntperfv;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;	
	
	CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr06_detcntperfv
	AS(	
	SELECT DISTINCT x1.num_ruc,
		   SUBSTR(x0.periodo,3,4)||SUBSTR(x0.periodo,1,2) as periodo,
		   CAST(x0.num_formul AS smallint) as cod_formul,
		   CAST(x0.num_ordope AS BIGINT) as num_ordope,
		   coalesce(x1.ind_presdj,0) as ind_presdj
	FROM ${TBL_T5409} x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	) WITH DATA NO PRIMARY INDEX;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


-------------------------PAGOS DIRECTOS  EN  MONGOBB------------------------------------------------------

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr06_detcntpermdb';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.tmp093168_kpigr06_detcntpermdb;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr06_detcntpermdb
    AS(	

	SELECT 
		DISTINCT  x1.num_ruc,
	      substr(num_perpago,3,4)||substr(num_perpago,1,2) as periodo,
	      cast(x0.cod_formul as smallint) as cod_formul,
	      x0.num_numorden as num_ordope,
	      coalesce(x1.ind_presdj,0) as ind_presdj
	FROM ${TBL_T5409_MDB} x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	) WITH DATA NO PRIMARY INDEX;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
					



/**************************************************************************************/
/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

  SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr06_cnorigen';
  .IF activitycount = 0 THEN .GOTO ok 
  
  DROP TABLE ${BD_STG}.tmp093168_kpigr06_cnorigen;
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;
  
  .label ok;
  
  CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr06_cnorigen AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_origen
    FROM ${BD_STG}.tmp093168_kpigr06_detcntpertr
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;
  
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------2. Conteo en FVirtual

  SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr06_cndestino1';
  .IF activitycount = 0 THEN .GOTO ok 
  
  DROP TABLE ${BD_STG}.tmp093168_kpigr06_cndestino1;
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;
  
  .label ok;
  CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr06_cndestino1 AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_destino1
    FROM ${BD_STG}.tmp093168_kpigr06_detcntperfv
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;
  
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

--------3 Conteo en MongoDB

  SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr06_cndestino2';
  .IF activitycount = 0 THEN .GOTO ok 
  
  DROP TABLE ${BD_STG}.tmp093168_kpigr06_cndestino2;
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;
  
  .label ok;
  CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr06_cndestino2 AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_destino2
    FROM ${BD_STG}.tmp093168_kpigr06_detcntpermdb
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;
  
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
		y0.numruc as num_ruc_trab,y0.periodo,y0.formul,y0.norden
	FROM (
		SELECT
            numruc,periodo,formul,norden 	
		FROM ${BD_STG}.tmp093168_kpigr06_detcntpertr
		EXCEPT ALL
		SELECT 
           num_ruc,periodo,cod_formul,num_ordope
        FROM ${BD_STG}.tmp093168_kpigr06_detcntperfv
	) y0
	) WITH DATA NO PRIMARY INDEX;

    .IF ERRORCODE <> 0 THEN .GOTO error_shell;

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_dif_${KPI_02}';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_02}	;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;
	

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_02} AS (
	SELECT DISTINCT 
		y0.num_ruc as num_ruc_trab,y0.periodo,y0.cod_formul,y0.num_ordope	
	FROM (
	    SELECT 
           num_ruc,periodo,cod_formul,num_ordope
        FROM ${BD_STG}.tmp093168_kpigr06_detcntperfv
		EXCEPT ALL
		SELECT 
           num_ruc,periodo,cod_formul,num_ordope
        FROM ${BD_STG}.tmp093168_kpigr06_detcntpermdb
	) y0
    ) WITH DATA NO PRIMARY INDEX;
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/************************ INSERTA CONTEOS A TABLAS DE DETALLE **************************/

 DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT  
  WHERE COD_KPI='${KPI_01}' AND FEC_CARGA=CURRENT_DATE;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

  INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
	SELECT
		'${PERIODO}',
		x0.ind_presdj,
		'${KPI_01}',
		CURRENT_DATE,
		case when x0.ind_presdj=0 then (select coalesce(sum(cant_per_origen),0) from ${BD_STG}.tmp093168_kpigr06_cnorigen) else 0 end as cant_origen,
		coalesce(x1.cant_per_destino1,0) as cant_destino,
		case when x0.ind_presdj=0 then 
        case when (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01})=0 then 1 else 0 end 
        end as ind_incuniv,
        case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_01}) END as cnt_regdif
	FROM 
	(
		select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
		from
		(
		select * from ${BD_STG}.tmp093168_kpigr06_cnorigen
		union all select 1,0 from (select '1' agr1) a
		union all select 0,0 from (select '0' agr0) b
		) y group by 1
	) x0
	LEFT JOIN ${BD_STG}.tmp093168_kpigr06_cndestino1 x1 
	ON x0.ind_presdj=x1.ind_presdj
   ;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;


  DELETE FROM ${BD_DQ}.T11908DETKPITRIBINT 
  WHERE COD_KPI='${KPI_02}' AND FEC_CARGA=CURRENT_DATE;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;


  INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT  
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
     SELECT '${PERIODO}',
			x0.ind_presdj,
			'${KPI_02}',
			CURRENT_DATE,
             x0.cant_per_destino1 AS cant_origen,
             case when x0.ind_presdj=0  then (select coalesce(sum(cant_per_destino2),0) from ${BD_STG}.tmp093168_kpigr06_cndestino2) else 0 end AS cant_destino,
			 case when x0.ind_presdj=0 then 
        	case when (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02})=0 then 1 else 0 end 
        	end as ind_incuniv,
       		case when x0.ind_presdj=0 then (select count(*) from ${BD_STG}.tmp093168_dif_${KPI_02}) END as cnt_regdif
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
          from
          (
            select * from ${BD_STG}.tmp093168_kpigr06_cndestino1
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN ${BD_STG}.tmp093168_kpigr06_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ;

  .IF ERRORCODE <> 0 THEN .GOTO error_shell;


    
/*********************************************************************/
    .EXPORT FILE ${FILE_KPI01};

	LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_01} 
	ORDER BY num_ruc_trab,periodo;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.EXPORT RESET;
    
    .EXPORT FILE ${FILE_KPI02};

    LOCK ROW FOR ACCESS
	SELECT * FROM ${BD_STG}.tmp093168_dif_${KPI_02}
	ORDER BY num_ruc_trab,periodo;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	.EXPORT RESET;

SEL CURRENT_TIMESTAMP;

/************************ BORRA TABLAS TEMPORALES **********************/
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_10;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_1651;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_09;	
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_tdevo;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_08;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_t869;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_01;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO_06;
DROP TABLE ${BD_STG}.TMP_KPI06_T03_0;
DROP TABLE ${BD_STG}.TMP_KPI06_T03;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_10;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_1651;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_09;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_tdev;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_08;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_t869;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO_06;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATPRICO;
DROP TABLE ${BD_STG}.TMP_KPI06_SIRATMEPECO;
DROP TABLE ${BD_STG}.tmp093168_kpigr06_detcntpertr;
DROP TABLE ${BD_STG}.tmp093168_kpigr06_detcntperfv;
DROP TABLE ${BD_STG}.tmp093168_kpigr06_detcntpermdb;
DROP TABLE ${BD_STG}.tmp093168_kpigr06_cnorigen;
DROP TABLE ${BD_STG}.tmp093168_kpigr06_cndestino1;
DROP TABLE ${BD_STG}.tmp093168_kpigr06_cndestino2;

/************************ FIN BORRA TABLAS TEMPORALES **********************/



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
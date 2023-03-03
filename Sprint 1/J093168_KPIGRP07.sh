#### Se ejecuta desde jobs DataStage  v27012023
#### ---------------------------------------------------------------------------
#### KPI07 Casilla 128 � Pagos directos de Quinta Categor�a Pagos directos
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
### $8 : Fecha de corte transaccional
### sh /work1/teradata/shells/093168/J093168_KPIGRP07.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD /work1/teradata/log/093168 2022 2023-01-31
### sh /work1/teradata/shells/093168/J093168_KPIGRP07.sh TDSUNAT usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG /work1/teradata/log/093168 2022 2023-01-31
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

CADENA_PERIODO="'${PERIODO}01','${PERIODO}02','${PERIODO}03','${PERIODO}04','${PERIODO}05','${PERIODO}06','${PERIODO}07','${PERIODO}08','${PERIODO}09','${PERIODO}10','${PERIODO}11','${PERIODO}12'"

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'
KPI_01='K007012022'
KPI_02='K007022022'
PATH_SALIDA_PLANOS='/work1/teradata/dat/093168'

FILE_KPI01=${PATH_SALIDA_PLANOS}'/DIF_'${KPI_01}'_CAS128_TRANVSFVIR_'${DATE}'.unl'
FILE_KPI02=${PATH_SALIDA_PLANOS}'/DIF_'${KPI_02}'_CAS128_FVIRVSMODB_'${DATE}'.unl'

NOM_SCRIPT='J093168_KPIGRP07.sh'

TBL_CRT=${BD_STG}'.CRT'
TBL_HSF=${BD_STG}'.HSF'
TBL_DBT=${BD_STG}'.DBT'
TBL_DOC=${BD_STG}'.DOC'
TBL_DDP=${BD_STG}'.DDP_DEPEN'
TBL_DB2=${BD_STG}'.DB2'
TBL_DEVOL=${BD_STG}'.devoluciones'
TBL_T869=${BD_STG}'.t869rei_cab'
TBL_T03=${BD_STG}'.T03DJCAB_depen'
TBL_T04=${BD_STG}'.T04DJDET_depen'
TBL_T5847=${BD_STG}'.t5847ctldecl'
TBL_T5410=${BD_STG}'.T5410CAS128'
TBL_T5410_MDB=${BD_STG}'.T5410CAS128_mongodb'
TBL_CAB_PRE_RES=${BD_STG}'.cab_pre_res'
TBL_T3386=${BD_STG}'.t3386doc_deu_com'
TBL_T1651=${BD_STG}'.t1651sol_comp'
TBL_DETALLEKPI=${BD_DQ}'.T11908DETKPITRIBINT'

rm -f ${FILE_01}
rm -f ${FILE_02}

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

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO_1';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_1;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_origen_01
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_1 as    
	(
		SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
		  FROM ${TBL_CRT}
		WHERE crt_perpag IN (${CADENA_PERIODO})
			 AND crt_codtri = '030501'
			 AND crt_indaju = '0'
			 AND crt_indpag IN (1,5)
			 AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
			 AND crt_estado <> '02'
		UNION
		SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
		  FROM ${TBL_CRT}
		WHERE crt_perpag IN (${CADENA_PERIODO})
			 AND crt_codtri = '030501'                                      
			 AND crt_tiptra = '2962'
			 AND crt_indaju = '1'
			 AND crt_indpag IN (1,5)
			 AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
			 AND crt_estado <> '02'                
	) WITH DATA NO PRIMARY INDEX; 

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


---Compensaciones (crt): ---

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO_2';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_2;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_origen_02
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_2 as    
	(
			SELECT crt_numruc numruc, crt_perpag perpag, 1648 formul, crt_docori numdoc 
			 FROM ${TBL_CRT}
			WHERE crt_perpag IN (${CADENA_PERIODO})
				 AND crt_codtri = '030501'
				 AND crt_tiptra = '1472'
				 AND crt_indaju = '1'
				 AND crt_imptri > 0
				 AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
	) WITH DATA NO PRIMARY INDEX; 			 
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

		INSERT INTO ${BD_STG}.TMP_KPI07_SIRATPRICO_1
		SELECT numruc, perpag, formul, numdoc  
		 FROM ${BD_STG}.TMP_KPI07_SIRATPRICO_2 a, ${TBL_CAB_PRE_RES} b
		WHERE b.num_res = a.numdoc
			AND b.cod_tip_doc = '023000'
			AND b.ind_est_pre = '1'
			AND b.ind_eta_pre = '2';
			
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

-- Exclusiones Pago en Proceso de compensaci�n


	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO_1651';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_1651;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_1651
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_1651 as    
	(
		SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, b.num_doc_ori numdoc  
		from ${BD_STG}.TMP_KPI07_SIRATPRICO_1 a, ${TBL_T1651} b
		WHERE a.numruc=b.num_ruc  
		AND b.ind_con_com IN ('3','4','5')
		AND b.cod_eta_sol IN ('01','02','03')
		AND b. cod_tri ='030501'
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
	

	
--compensaciones

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO_06';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_06;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_origen_06
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_06 as    
	(

		SELECT a.numruc, a.perpag, a.formul, a.numdoc 
		FROM ${BD_STG}.TMP_KPI07_SIRATPRICO_1 a
		LEFT JOIN ${BD_STG}.TMP_KPI07_SIRATPRICO_1651 b
		ON a.numruc=b.numruc  and a.formul=b.formul and a.numdoc= b.numdoc
		WHERE b.numruc  IS NULL
		AND b.formul IS NULL
		AND b.numdoc IS NULL
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
		
--Exclusiones Pago en Proceso de devoluci�n

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO_tdev';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_tdev;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_tdev as    --t_dev
	(
		SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_aso formul, b.num_doc_aso numdoc
		from ${BD_STG}.TMP_KPI07_SIRATPRICO_1 a, ${TBL_DEVOL} b
		WHERE a.numruc=b.num_ruc  
		AND b.cod_tip_sol = '02'
		AND b.ind_est_dev IN ('0','3')
		AND b.ind_res_dev IN ('0','F')
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
		
--devoluciones 

	INSERT INTO ${BD_STG}.TMP_KPI07_SIRATPRICO_06
	SELECT a.numruc, a.perpag, a.formul, a.numdoc 
	FROM ${BD_STG}.TMP_KPI07_SIRATPRICO_1 a
	LEFT JOIN ${BD_STG}.TMP_KPI07_SIRATPRICO_tdev b
	ON a.numruc=b.numruc  and a.formul=b.formul  and a.numdoc= b.numdoc
	WHERE b.numruc    IS NULL
	AND b.formul  IS NULL
	AND b.numdoc IS NULL;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--Exclusiones Pago en Proceso de reimputaci�n

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO_treimp';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_treimp;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_reimp
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_treimp as    
	(		
		SELECT  a.numruc, a.perpag, b.cod_for formul, b.num_doc numdoc 
		from ${BD_STG}.TMP_KPI07_SIRATPRICO_1 a, ${TBL_T869} b
		WHERE a.numruc=b.num_ruc  
		AND b.cod_for_rei ='4715'
		AND b.ind_aplica = '0'
		AND b.ind_motivo NOT IN ('0','9')
	) WITH DATA NO PRIMARY INDEX; 
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
		
		INSERT INTO ${BD_STG}.TMP_KPI07_SIRATPRICO_06			
		SELECT a.numruc, a.perpag, a.formul, a.numdoc 
		FROM ${BD_STG}.TMP_KPI07_SIRATPRICO_1 a
		LEFT JOIN ${BD_STG}.TMP_KPI07_SIRATPRICO_treimp b
		ON a.numruc=b.numruc AND a.formul=b.formul and a.numdoc= b.numdoc
		WHERE b.formul IS NULL
		AND b.numdoc IS NULL;
		
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 			


-- Compensaciones a valores (crt):


	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO_05';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_05;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_05 as    --t_origen_05
	(	
	SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc 
	  FROM ${TBL_T3386} a, ${TBL_CAB_PRE_RES} b
	 WHERE a.cod_tri_deu = '030501'
		  AND a.num_pre_res = b.num_pre_res
		  AND b.ind_est_pre = '1'
		  AND b.ind_eta_pre = '2'
		  AND a.ind_tip_deu = '01'
		  AND a.cod_tip_cal IN ('023001', '023002')
	) WITH DATA NO PRIMARY INDEX; 			  
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 		


	INSERT INTO ${BD_STG}.TMP_KPI07_SIRATPRICO_06
	SELECT a.num_ruc numruc, per_tri_des perpag,a.cod_for,a.nro_orden
	FROM ${TBL_T1651} a ,${BD_STG}.TMP_KPI07_SIRATPRICO_05 b
	WHERE cod_for = '1648'
		 AND a.nro_orden = b.numdoc 
		 AND a.num_ruc = b.numruc 
		 AND a.per_tri_des IN (${CADENA_PERIODO});

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATPRICO';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
	
	--t_origen_03
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO as    
	(	

		SELECT DISTINCT numruc, perpag,formul ,numdoc 
		 FROM ${BD_STG}.TMP_KPI07_SIRATPRICO_06
	) WITH DATA NO PRIMARY INDEX; 

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
		
--- RESULTADO : TABLA ${BD_STG}.TMP_KPI07_SIRATPRICO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT PRICO   	

/******************************* MEPECO SIRAT *********************************/
	-- PAGOS DDJJ 0616


	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO_01';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_01;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
				
				--t_origen_01
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_01 
	AS(

			SELECT crt_numruc AS numruc, crt_perpag AS perpag, crt_formul AS formul,
			       crt_ndocpa as numdoc
			 FROM ${TBL_CRT}
			WHERE crt_perpag IN (${CADENA_PERIODO})
				 AND crt_codtri = '030501'
				 AND crt_formul NOT IN (1083,1683,116,616)
				 AND crt_indaju = '0'
				 AND crt_indpag IN (1,5)
				 AND crt_estado <> '02'
				 AND crt_imptri > 0
				 AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
			GROUP BY 1,2,3,4

	) WITH DATA NO PRIMARY INDEX ; 
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;
	
	-- Exclusiones pago en proceso de compensacion 			

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO_1651';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_1651;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_1651 as    --t_1651
	(
		SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, b.num_doc_ori numdoc  
		from ${BD_STG}.TMP_KPI07_SIRATMEPECO_01 a, ${TBL_T1651} b
		WHERE a.numruc=b.num_ruc  
		AND b.ind_con_com IN ('3','4','5')
		AND b.cod_eta_sol IN ('01','02','03')
		AND b. cod_tri ='030501'
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	--compensaciones

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO_06';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_06;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_06 as    --t_origen_06
	(

		SELECT a.numruc, a.perpag, a.formul, a.numdoc 
		FROM ${BD_STG}.TMP_KPI07_SIRATMEPECO_01 a
		LEFT JOIN ${BD_STG}.TMP_KPI07_SIRATMEPECO_1651 b
		ON a.numruc=b.numruc  and a.formul=b.formul and a.numdoc= b.numdoc
		WHERE b.numruc  IS NULL
		AND b.formul IS NULL
		AND b.numdoc IS NULL
	) WITH DATA NO PRIMARY INDEX; 			 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 		


		--Exclusiones Pago en Proceso de devoluci�n

		
	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO_tdev';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_tdev;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;
			
			--t_dev
	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_tdev as    
	(
		SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_aso formul, b.num_doc_aso numdoc
		from ${BD_STG}.TMP_KPI07_SIRATMEPECO_01 a, ${TBL_DEVOL} b
		WHERE a.numruc=b.num_ruc  
		AND b.cod_tip_sol = '02'
		AND b.ind_est_dev IN ('0','3')
		AND b.ind_res_dev IN ('0','F')
	) WITH DATA NO PRIMARY INDEX; 			 	

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

		--devoluciones 
		
		INSERT INTO ${BD_STG}.TMP_KPI07_SIRATMEPECO_06
		SELECT a.numruc, a.perpag, a.formul, a.numdoc 
		FROM ${BD_STG}.TMP_KPI07_SIRATMEPECO_01 a
		LEFT JOIN ${BD_STG}.TMP_KPI07_SIRATMEPECO_tdev b
		ON a.numruc=b.numruc  and a.formul=b.formul  and a.numdoc= b.numdoc
		WHERE b.numruc IS NULL
		AND b.formul IS NULL
		AND b.numdoc IS NULL;

		.IF ERRORCODE <> 0 THEN .GOTO error_shell; 					
				
		--Exclusiones Pago en Proceso de reimputaci�n


		SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO_treimp';
		.IF activitycount = 0 THEN .GOTO ok 

		DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_treimp;

		.IF ERRORCODE <> 0 THEN .GOTO error_shell;

		.label ok;

		CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_treimp as    --t_reimp
		(		
		
			SELECT  a.numruc, a.perpag, b.cod_for formul, b.num_doc numdoc
			from ${BD_STG}.TMP_KPI07_SIRATMEPECO_01 a, ${TBL_T869} b
			WHERE a.numruc=b.num_ruc  
			AND b.cod_for_rei ='4715'
			AND b.ind_aplica = '0'
			AND b.ind_motivo NOT IN ('0','9')
			
		) WITH DATA NO PRIMARY INDEX; 
		
		.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
			
			INSERT INTO ${BD_STG}.TMP_KPI07_SIRATMEPECO_06			
			SELECT a.numruc, a.perpag, a.formul, a.numdoc 
			FROM ${BD_STG}.TMP_KPI07_SIRATMEPECO_01 a
			LEFT JOIN ${BD_STG}.TMP_KPI07_SIRATMEPECO_treimp b
			ON a.numruc=b.numruc AND a.formul=b.formul and a.numdoc= b.numdoc
			WHERE b.formul IS NULL
			AND b.numdoc IS NULL
			AND b.numruc IS NULL;
			
		.IF ERRORCODE <> 0 THEN .GOTO error_shell; 			
		
				
	-- COMPENSACIONES CRT


	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO_02';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_02;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_02 as    --t_origen_02
	(		
		SELECT crt_numruc AS numruc, crt_perpag AS perpag, 1648 AS formul, crt_docori AS numdoc
		FROM ${TBL_CRT}
		WHERE crt_perpag IN (${CADENA_PERIODO})
			 AND crt_codtri = '030501'
			 AND crt_tiptra = '1472'
			 AND crt_indaju = '1'
			 AND crt_imptri > 0
			 AND crt_fecpag <= CAST('${FCH_PAGO}' AS DATE FORMAT 'YYYY-MM-DD')
	) WITH DATA NO PRIMARY INDEX; 
			
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	INSERT INTO  ${BD_STG}.TMP_KPI07_SIRATMEPECO_01
	SELECT numruc, perpag, formul, numdoc  
	 FROM ${BD_STG}.TMP_KPI07_SIRATMEPECO_02 a, ${TBL_CAB_PRE_RES} b
	WHERE b.num_res = a.numdoc
		AND b.cod_tip_doc = '023000'
		AND b.ind_est_pre = '1'
		AND b.ind_eta_pre = '2';
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;


	-- COMPENSACIONES A VALORES (CRT)


	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO_05';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_05;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_05 as    --t_origen_05
	(		
		SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc
		FROM ${TBL_T3386} a, ${TBL_CAB_PRE_RES} b
		WHERE a.cod_tri_deu = '030501'
		AND a.num_pre_res = b.num_pre_res
		AND b.ind_est_pre = '1'
		AND b.ind_eta_pre = '2'
		AND a.ind_tip_deu = '01'
		AND a.cod_tip_cal IN ('023001','023002')
						  
	) WITH DATA NO PRIMARY INDEX; 
			
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;			

	INSERT INTO  ${BD_STG}.TMP_KPI07_SIRATMEPECO_01
	SELECT a.num_ruc numruc, a.per_tri_des perpag,a.cod_for,a.nro_orden
	FROM ${TBL_T1651} a , ${BD_STG}.TMP_KPI07_SIRATMEPECO_05 b
	WHERE a.cod_for = '1648'
	AND a.nro_orden = b.numdoc 
	AND a.num_ruc = b.numruc 
	AND a.per_tri_des IN (${CADENA_PERIODO});

----  RESUMEN SIRATMEPECO

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'TMP_KPI07_SIRATMEPECO';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO as    --t_origen_03
	(	

		SELECT DISTINCT numruc,perpag,formul ,numdoc
		 FROM ${BD_STG}.TMP_KPI07_SIRATMEPECO_01
	) WITH DATA NO PRIMARY INDEX; 

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
					
-- RESULTADO : TABLA ${BD_STG}.TMP_KPI07_SIRATMEPECO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT MEPECO
/**************************FIN MEPECO SIRAT ****************************************/

/***********************************************************************************************************************/	
/***********************************************************************************************************************/
-------------------------PAGOS DIRECTOS EN TRANSACCIONAL PRICO Y MEPECO--------------------------------------------------

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr07_detcntpertr';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.tmp093168_kpigr07_detcntpertr;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr07_detcntpertr  
	AS(
        SELECT
         x0.numruc,x0.perpag as periodo,x0.formul,x0.numdoc as norden,
         coalesce(x1.ind_presdj,0) as ind_presdj
		FROM(
            SELECT a.numruc, a.perpag,a.formul,a.numdoc 
    		FROM ${BD_STG}.TMP_KPI07_SIRATPRICO a , ${TBL_DDP} b 
    		WHERE a.numruc=b.ddp_numruc 
    		UNION 
    		SELECT a.numruc,a.perpag,a.formul ,a.numdoc 
    		FROM ${BD_STG}.TMP_KPI07_SIRATMEPECO a , ${TBL_DDP} b 
    		WHERE a.numruc=b.ddp_numruc
          ) x0
          INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.numruc = x1.num_ruc		
	) WITH DATA NO PRIMARY INDEX ; 			

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


--------------------------PAGOS DIRECTOS  EN  FVIRTUAL------------------------------------------------------

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr07_detcntperfv';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.tmp093168_kpigr07_detcntperfv;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;	
	
	CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr07_detcntperfv
	AS(	
	SELECT DISTINCT x1.num_ruc,
		   SUBSTR(x0.periodo,3,4)||SUBSTR(x0.periodo,1,2) as periodo,
		   CAST(x0.num_formul AS smallint) as cod_formul,
		   CAST(x0.num_ordope AS BIGINT) as num_ordope,
		   coalesce(x1.ind_presdj,0) as ind_presdj
	FROM ${TBL_T5410} x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	) WITH DATA NO PRIMARY INDEX;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


-------------------------PAGOS DIRECTOS  EN  MONGOBB------------------------------------------------------

	SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr07_detcntpermdb';
	.IF activitycount = 0 THEN .GOTO ok 

	DROP TABLE ${BD_STG}.tmp093168_kpigr07_detcntpermdb;
	.IF ERRORCODE <> 0 THEN .GOTO error_shell;

	.label ok;

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr07_detcntpermdb
    AS(	

	SELECT DISTINCT
		  x1.num_ruc,
	      substr(num_perpago,3,4)||substr(num_perpago,1,2) as periodo,
	      cast(x0.cod_formul as smallint) as cod_formul,
	      x0.num_numorden as num_ordope,
	      coalesce(x1.ind_presdj,0) as ind_presdj
	FROM ${TBL_T5410_MDB} x0
	INNER JOIN ${BD_STG}.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	) WITH DATA NO PRIMARY INDEX;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 
					



/**************************************************************************************/
/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

  SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr07_cnorigen';
  .IF activitycount = 0 THEN .GOTO ok 
  
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_cnorigen;
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;
  
  .label ok;
  
  CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr07_cnorigen AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_origen
    FROM ${BD_STG}.tmp093168_kpigr07_detcntpertr
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;
  
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;


---------2. Conteo en FVirtual

  SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr07_cndestino1';
  .IF activitycount = 0 THEN .GOTO ok 
  
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_cndestino1;
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;
  
  .label ok;
  CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr07_cndestino1 AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_destino1
    FROM ${BD_STG}.tmp093168_kpigr07_detcntperfv
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;
  
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;

--------3 Conteo en MongoDB

  SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpigr07_cndestino2';
  .IF activitycount = 0 THEN .GOTO ok 
  
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_cndestino2;
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;
  
  .label ok;
  CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpigr07_cndestino2 AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_destino2
    FROM ${BD_STG}.tmp093168_kpigr07_detcntpermdb
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;
  
  .IF ERRORCODE <> 0 THEN .GOTO error_shell;


/************************ INSERTA CONTEOS A TABLAS DE DETALLE **************************/

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
             case when x0.ind_presdj=0 then (select coalesce(sum(cant_per_origen),0) from ${BD_STG}.tmp093168_kpigr07_cnorigen) else 0 end as cant_origen,
             coalesce(x1.cant_per_destino1,0) as cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
          from
          (
            select * from ${BD_STG}.tmp093168_kpigr07_cnorigen
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN ${BD_STG}.tmp093168_kpigr07_cndestino1 x1 
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
             case when x0.ind_presdj=0  then (select coalesce(sum(cant_per_destino2),0) from ${BD_STG}.tmp093168_kpigr07_cndestino2) else 0 end AS cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
          from
          (
            select * from ${BD_STG}.tmp093168_kpigr07_cndestino1
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN ${BD_STG}.tmp093168_kpigr07_cndestino2 x1 
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
  		y0.numruc as num_ruc_trab,y0.periodo,y0.formul,y0.norden
  	FROM (
  		SELECT
              numruc,periodo,formul,norden 	
  		FROM ${BD_STG}.tmp093168_kpigr07_detcntpertr
  		EXCEPT ALL
  		SELECT 
             num_ruc,periodo,cod_formul,num_ordope
          FROM ${BD_STG}.tmp093168_kpigr07_detcntperfv
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
          FROM ${BD_STG}.tmp093168_kpigr07_detcntperfv
  		EXCEPT ALL
  		SELECT 
             num_ruc,periodo,cod_formul,num_ordope
          FROM ${BD_STG}.tmp093168_kpigr07_detcntpermdb
  	) y0
      ) WITH DATA NO PRIMARY INDEX;
  	
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


/*********************** FIN INSERTA CONTEOS A TABLAS DE DETALLE **********************/
/************************ BORRA TABLAS TEMPORALES **********************/

  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_1;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_2;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_1651;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_06;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_tdev;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_treimp;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO_05;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATPRICO;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_01;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_1651;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_06;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_tdev;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_treimp;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_02;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO_05;
  DROP TABLE ${BD_STG}.TMP_KPI07_SIRATMEPECO;
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_detcntpertr;
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_detcntperfv;
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_detcntpermdb;
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_cnorigen;
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_cndestino1;
  DROP TABLE ${BD_STG}.tmp093168_kpigr07_cndestino2;


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

if [ $CODRET = 0 ]; then
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


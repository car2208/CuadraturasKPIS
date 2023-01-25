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
### sh J093168_KPIGRP20.sh tdtp01s2 usr_carga_desa twusr_carga_desa bddwedqd bddwestgd /work1/teradata/log/093168 2022
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
KPI_01='K020012022'
KPI_02='K020022022'
FILE_KPI01='/work1/teradata/dat/093168/DIFF_'${KPI_01}'_'${DATE}'.unl'
FILE_KPI02='/work1/teradata/dat/093168/DIFF_'${KPI_02}'_'${DATE}'.unl'


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
/************************************TRANSACCIONALES*****************************************/
/*========================================================================================= */

-- Cantidad de comprobantes válidos de gastos deducibles para el Rubro EsSalud – Trabajadores del Hogar

--UNIVERSO PARA EL ORIGEN: Servidor infp25s2 / Online: deduc_01 / BD: gastodeduc

DROP TABLE ${BD_STG}.tmpCompVali_GDEsSaTrabHogar020;
CREATE MULTISET TABLE ${BD_STG}.tmpCompVali_GDEsSaTrabHogar020 as
(
	Select a.num_ruc, count(a.num_ruc) cantidadTran
	From ${BD_STG}.t8156cpgastodeduc a 	
	Where a.ann_ejercicio = '${PERIODO}' 
		and a.ind_tip_gasto = '04' 
		and a.ind_estado = '1'
		and substr(a.num_partida,1,4) = '${PERIODO}'
		and fec_doc >= '${PERIODO}-01-01' and fec_doc <= '${NPER}-01-31'
	group by 1
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--select * from ${BD_STG}.tmpCompVali_GDEsSaTrabHogar020 sample 100;
--select count(1) from ${BD_STG}.tmpCompVali_GDEsSaTrabHogar020; --


/*========================================================================================= */
/**************************************F-VIRTUAL*********************************************/
/*========================================================================================= */

DROP TABLE ${BD_STG}.tmpGeneralSinPresentarDJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpGeneralSinPresentarDJ020 as 
(
	SELECT 
		num_ruc,
		MAX(num_sec) as num_sec
	FROM ${BD_STG}.t5847ctldecl 
	WHERE num_ejercicio = ${PERIODO}
		AND num_formul = '0709' 
		AND ind_actual = '1' 
		AND ind_estado = '0' 
		AND ind_proceso = '1'
	GROUP BY 1
) with data no primary INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


--SELECT * FROM ${BD_STG}.tmpGeneralSinPresentarDJ020 SAMPLE 100;
--SELECT COUNT(1) FROM ${BD_STG}.tmpGeneralSinPresentarDJ020; --10132412

------------Presentaron DJ----------------------

DROP TABLE ${BD_STG}.tmpPresentaronDJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpPresentaronDJ020 as 
(
	SELECT 	num_ruc,
			MAX(num_sec) as num_sec 
	FROM ${BD_STG}.t5847ctldecl 
	WHERE num_ejercicio = ${PERIODO}
		AND num_formul = '0709' 
		AND ind_estado = '2'
	GROUP BY 1
)  with data no primary INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


--SELECT * FROM ${BD_STG}.tmpPresentaronDJ020 SAMPLE 100;
--SELECT COUNT(*) FROM ${BD_STG}.tmpPresentaronDJ020; --75


------------NO presentaron DJ-------------------------

DROP TABLE ${BD_STG}.tmpNOPresentaronDJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpNOPresentaronDJ020 as 
(
	SELECT 	num_ruc, 
			num_sec 
	FROM ${BD_STG}.tmpGeneralSinPresentarDJ020 
	WHERE num_ruc NOT IN (SELECT num_ruc FROM ${BD_STG}.tmpPresentaronDJ020)
)  WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--SELECT * FROM ${BD_STG}.tmpNOPresentaronDJ020 SAMPLE 100;
--SELECT COUNT(1) FROM ${BD_STG}.tmpNOPresentaronDJ020; --10132412


----------------A.-Cuando el contribuyente aún no ha presentado su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------

DROP TABLE ${BD_STG}.tmpCompValFVirtual_SinDJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpCompValFVirtual_SinDJ020 as
(
	Select a.num_ruc, count(a.num_ruc) cantidadFV0
	From ${BD_STG}.t12734cas514det a 
    Inner join ${BD_STG}.tmpNOPresentaronDJ020 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '04'
		and a.ind_archpers = '1'
		and a.ind_est_archpers='0'
		and a.ind_est_formvirt ='0'
		and a.fec_comprob >= '${PERIODO}-01-01' and a.fec_comprob <= '${NPER}-01-31'
	group by a.num_ruc

)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


--select * from ${BD_STG}.tmpCompValFVirtual_SinDJ020 sample 100;
--select count(1) from ${BD_STG}.tmpCompValFVirtual_SinDJ020; --


----------------B.Cuando el contribuyente ya presentó su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------

DROP TABLE ${BD_STG}.tmpCompValFVirtual_ConDJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpCompValFVirtual_ConDJ020 as
(
	Select a.num_ruc, count(a.num_ruc) cantidadFV1
	From ${BD_STG}.t12734cas514det a 
	Inner join ${BD_STG}.tmpPresentaronDJ020 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '04'
		and a.ind_archpers = '1' 
		and a.ind_est_archpers='0'
		and a.ind_est_formvirt ='0'
		and a.fec_comprob >= '${PERIODO}-01-01' and a.fec_comprob <= '${NPER}-01-31'
	Group by a.num_ruc
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--select * from ${BD_STG}.tmpCompValFVirtual_ConDJ020 sample 100;
--select count(1) from ${BD_STG}.tmpCompValFVirtual_ConDJ020; --


DROP TABLE ${BD_STG}.tmpUnivCompValFV_DJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpUnivCompValFV_DJ020 as 
(
	Select num_ruc, cantidadFV0 cantidadFV, 0 ind_DjFV  
	from ${BD_STG}.tmpCompValFVirtual_SinDJ020 
	Union
	Select num_ruc, cantidadFV1 cantidadFV, 1 ind_DjFV 
	from ${BD_STG}.tmpCompValFVirtual_ConDJ020 
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--select * from ${BD_STG}.tmpUnivCompValFV_DJ020 sample 100;
--select count(1) from ${BD_STG}.tmpUnivCompValFV_DJ020; --


/*========================================================================================= */
/**************************************MONGO DB*********************************************/
/*========================================================================================= */

--Comprobantes VALIDOS Con DJ y Sin DJ en MongoDB 

DROP TABLE ${BD_STG}.tmpCompValiMongo_DJTot020;
CREATE MULTISET TABLE ${BD_STG}.tmpCompValiMongo_DJTot020 as 
(
	Select 
	num_ruc,	
	count(num_ruc) cnt_ValiMong
	from ${BD_STG}.t12734cas514det_mongodb
	where num_eje = '${PERIODO}' 
		and num_form = '0709'
		and cod_tip_gasto = '04'
		and ind_archpers = '1'
		and ind_est_archpers = '0'
		and ind_est_formvirt ='0' 
		and fec_comprob >= '${PERIODO}-01-01' AND fec_comprob <= '${NPER}-01-31'
	group by num_ruc
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--select * from ${BD_STG}.tmpCompValiMongo_DJTot020 sample 100;
--select count(1) from ${BD_STG}.tmpCompValiMongo_DJTot020; --


/*=======Hallar la marca con DJ y sin DJ en Transaccionales y Mongo DB==========*/

DROP TABLE ${BD_STG}.tmpUnivValiTranFV_DJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpUnivValiTranFV_DJ020 as --104
(
	Select
		a.num_ruc num_rucTra, 
		b.num_ruc num_rucFV,
		coalesce(b.ind_DjFV,0) ind_DJ,
		a.cantidadTran,
		coalesce(b.cantidadFV,0) cantidadFV
	from ${BD_STG}.tmpCompVali_GDEsSaTrabHogar020 a 
	Left join ${BD_STG}.tmpUnivCompValFV_DJ020 b on b.num_ruc = a.num_ruc 
)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--select * from ${BD_STG}.tmpUnivValiTranFV_DJ020 sample 100;
--select count(1) from ${BD_STG}.tmpUnivValiTranFV_DJ020; --


DROP TABLE ${BD_STG}.tmpUnivValiFVMon_DJ020;
CREATE MULTISET TABLE ${BD_STG}.tmpUnivValiFVMon_DJ020 as --104
(
	Select
		a.num_ruc num_rucFV, 
		b.num_ruc num_rucMongo,
		coalesce(a.ind_DjFV,0) ind_DJ,
		a.cantidadFV,
		coalesce(b.cnt_ValiMong,0) cnt_ValiMong
	from ${BD_STG}.tmpUnivCompValFV_DJ020 a
	Left join ${BD_STG}.tmpCompValiMongo_DJTot020 b on b.num_ruc = a.num_ruc

)
WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

--select * from ${BD_STG}.tmpUnivValiFVMon_DJ020 sample 100;
--select count(1) from ${BD_STG}.tmpUnivValiFVMon_DJ020; --


/*========================================================================================= */
/*********************************INSERTA EN TABLA HECHOS ***********************************/
/*========================================================================================= */

	---INSERTA VALIDOS C/S DJ PARA LA 1ERA COMPARACION (TRANS VS FVIRTUAL)
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 	
		'${NPER}',
		z.ind_DJ,
	    '${KPI_01}',
	    CURRENT_DATE,
	    SUM(z.cantidadTran),
	    SUM(z.cantidadFV)
	FROM
		(
			Select
			num_rucTra, 
			num_rucFV, 
			ind_DJ, 
			cantidadTran,  
			cantidadFV
			from ${BD_STG}.tmpUnivValiTranFV_DJ020		
		) z
	GROUP BY 1,2,3,4
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 



	---INSERTA VALIDOS C/S DJ PARA LA 2DA COMPARACION (FVIRTUAL VS MONGODB)
	INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN ,MTO_REGIDESTINO)
	SELECT 	
		'${NPER}',
		z.ind_DJ,
	    '${KPI_02}',
	    CURRENT_DATE,
	    SUM(z.cantidadFV),
	    SUM(z.cnt_ValiMong)
	FROM
		(
			Select 
			num_rucFV, 
			num_rucMongo, 
			ind_DJ, 
			cantidadFV, 
			cnt_ValiMong
			from ${BD_STG}.tmpUnivValiFVMon_DJ020
		) z
	GROUP BY 1,2,3,4
	;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

	
/*========================================================================================= */
/********************************* HALLAR LAS DIFERENCIAS ***********************************/
/*========================================================================================= */	

	--Diferencias entre Transaccional y F-Virtual de los comprobantes VALIDOS
	Select
		num_rucTra, 
		num_rucFV, 
		cantidadTran,  
		cantidadFV,
		ind_DJ
	from ${BD_STG}.tmpUnivValiTranFV_DJ020; 	
	
	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


	--Diferencias entre F-Virtual y MongoDB de los comprobantes VALIDOS	
	Select 
		num_rucFV, 
		num_rucMongo, 
		cantidadFV, 
		cnt_ValiMong,
		ind_DJ
	from ${BD_STG}.tmpUnivValiFVMon_DJ020;

	.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


	
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
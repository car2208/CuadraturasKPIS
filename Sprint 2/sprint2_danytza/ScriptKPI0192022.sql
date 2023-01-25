/*========================================================================================= */
/************************************TRANSACCIONALES*****************************************/
/*========================================================================================= */

-- Cantidad de comprobantes válidos de gastos deducibles para el Rubro Alquiler Inmuebles:

--UNIVERSO PARA EL ORIGEN: Servidor infp25s2 / Online: deduc_01 / BD: gastodeduc
-- DROP TABLE BDDWESTG.tmpCompVali_GDAlqInm019;
CREATE MULTISET TABLE BDDWESTG.tmpCompVali_GDAlqInm019 as
(

	Select a.num_ruc, sum(mto_deduccion_fin) mto_deduccion_fin
	From BDDWESTG.t8156cpgastodeduc a 
	Left join BDDWESTG.ddp b on a.num_ruc_emisor = b.ddp_numruc 
	Where a.ann_ejercicio = '2022' 
		and a.ind_tip_gasto = '01' 
		and coalesce(a.num_contrato,'') in ('','-','1')
		and substr(a.num_partida,1,4) = '2022'
		and a.ind_estado = '1'
		and fec_doc >= '2022-01-01' and fec_doc <= '2023-01-31'
	group by 1

)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTG.tmpCompVali_GDAlqInm019 sample 100;
--select count(1) from BDDWESTG.tmpCompVali_GDAlqInm019; --

/*========================================================================================= */
/**************************************F-VIRTUAL*********************************************/
/*========================================================================================= */

--DROP TABLE BDDWESTG.tmpGeneralSinPresentarDJ019;
CREATE MULTISET TABLE BDDWESTG.tmpGeneralSinPresentarDJ019 as 
(
	SELECT 
		num_ruc,
		MAX(num_sec) as num_sec
	FROM BDDWESTG.t5847ctldecl 
	WHERE num_ejercicio = 2022
		AND num_formul = '0709' 
		AND ind_actual = '1' 
		AND ind_estado = '0' 
		AND ind_proceso = '1'
	GROUP BY 1
) with data no primary INDEX;

--SELECT * FROM BDDWESTG.tmpGeneralSinPresentarDJ019 SAMPLE 100;
--SELECT COUNT(1) FROM BDDWESTG.tmpGeneralSinPresentarDJ019; --10132412

------------Presentaron DJ----------------------

--DROP TABLE BDDWESTG.tmpPresentaronDJ019;
CREATE MULTISET TABLE BDDWESTG.tmpPresentaronDJ019 as 
(
	SELECT 	num_ruc,
			MAX(num_sec) as num_sec 
	FROM BDDWESTG.t5847ctldecl 
	WHERE num_ejercicio = 2022
		AND num_formul = '0709' 
		AND ind_estado = '2'
	GROUP BY 1
)  with data no primary INDEX;


--SELECT * FROM BDDWESTG.tmpPresentaronDJ019 SAMPLE 100;
--SELECT COUNT(*) FROM BDDWESTG.tmpPresentaronDJ019; --75


------------NO presentaron DJ-------------------------

--DROP TABLE BDDWESTG.tmpNOPresentaronDJ019;
CREATE MULTISET TABLE BDDWESTG.tmpNOPresentaronDJ019 as 
(
	SELECT 	num_ruc, 
			num_sec 
	FROM BDDWESTG.tmpGeneralSinPresentarDJ019 
	WHERE num_ruc NOT IN (SELECT num_ruc FROM BDDWESTG.tmpPresentaronDJ019)
)  WITH DATA NO PRIMARY INDEX;

--SELECT * FROM BDDWESTG.tmpNOPresentaronDJ019 SAMPLE 100;
--SELECT COUNT(1) FROM BDDWESTG.tmpNOPresentaronDJ019; --10132412


----------------A.-Cuando el contribuyente aún no ha presentado su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------
-- DROP TABLE BDDWESTG.tmpCompValFVirtual_SinDJ019;
CREATE MULTISET TABLE BDDWESTG.tmpCompValFVirtual_SinDJ019 as
(
	Select a.num_ruc, SUM(a.mto_deduccion) mto_deduccion --a.num_ruc, count(a.num_ruc) 
	From BDDWESTG.t12734cas514det a 
    Inner join BDDWESTG.tmpNOPresentaronDJ019 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '01'
		and a.ind_archpers = '1'  
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
	group by a.num_ruc

)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTG.tmpCompValFVirtual_SinDJ019 sample 100;
--select count(1) from BDDWESTG.tmpCompValFVirtual_SinDJ019; --


----------------B.Cuando el contribuyente ya presentó su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------
-- DROP TABLE BDDWESTG.tmpCompValFVirtual_ConDJ019;
CREATE MULTISET TABLE BDDWESTG.tmpCompValFVirtual_ConDJ019 as
(
	Select a.num_ruc, SUM(a.mto_deduccion) mto_deduccion --a.num_ruc, count(a.num_ruc) 
	From BDDWESTG.t12734cas514det a 
	Inner join BDDWESTG.tmpPresentaronDJ019 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '01'
		and a.ind_archpers = '1'  
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
	Group by a.num_ruc
)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTG.tmpCompValFVirtual_ConDJ019 sample 100;
--select count(1) from BDDWESTG.tmpCompValFVirtual_ConDJ019; --


-- DROP TABLE BDDWESTGD.tmpUnivCompValFV_DJ019;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivCompValFV_DJ019 as 
(
	Select num_ruc, mto_deduccion, 0 ind_DjFV  
	from BDDWESTGD.tmpCompValFVirtual_SinDJ019 
	Union
	Select num_ruc, mto_deduccion, 1 ind_DjFV 
	from BDDWESTGD.tmpCompValFVirtual_ConDJ019 
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivCompValFV_DJ019 sample 100;
--select count(1) from BDDWESTGD.tmpUnivCompValFV_DJ019; --


/*========================================================================================= */
/**************************************MONGO DB*********************************************/
/*========================================================================================= */
--Comprobantes VALIDOS Con DJ y Sin DJ en MongoDB 
-- DROP TABLE BDDWESTGD.tmpCompValiMongo_DJTot019;
CREATE MULTISET TABLE BDDWESTGD.tmpCompValiMongo_DJTot019 as 
(
	Select 
	num_ruc,	
	sum(mto_deduccion) mto_deduccionMon
	from BDDWESTGD.t12734cas514det_mongodb
	where num_eje = '2022' 
		and num_form = '0709'
		and cod_tip_gasto = '01'
		and ind_archpers = '1'  
		and fec_comprob >= '2022-01-01' AND fec_comprob <= '2023-01-31'
	group by num_ruc
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpCompValiMongo_DJTot019 sample 100;
--select count(1) from BDDWESTGD.tmpCompValiMongo_DJTot019; --


/*=======Hallar la marca con DJ y sin DJ en Transaccionales y Mongo DB==========*/

-- DROP TABLE BDDWESTGD.tmpUnivValiTranFV_DJ019;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivValiTranFV_DJ019 as --104
(
	Select
		a.num_ruc num_rucTra, 
		b.num_ruc num_rucFV,
		coalesce(b.ind_DjFV,0) ind_DJ,
		a.mto_deduccion_fin mto_deduccionTran,
		coalesce(b.mto_deduccion,0) mto_deduccionFV
	BDDWESTGD.tmpCompVali_GDAlqInm019 a 
	Left join BDDWESTGD.tmpUnivCompValFV_DJ019 b on b.num_ruc = a.num_ruc --num_ruc, mto_deduccion, 0 ind_DjFV  
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivValiTranFV_DJ019 sample 100;
--select count(1) from BDDWESTGD.tmpUnivValiTranFV_DJ019; --



-- DROP TABLE BDDWESTGD.tmpUnivValiFVMon_DJ019;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivValiFVMon_DJ019 as --104
(
	Select
		a.num_ruc num_rucFV, 
		b.num_ruc num_rucMongo,
		coalesce(a.ind_DjFV,0) ind_DJ,
		a.mto_deduccion mto_deduccionFV,
		coalesce(b.mto_deduccionMon,0) mto_deduccionMon
	BDDWESTGD.tmpUnivCompValFV_DJ019 a
	Left join BDDWESTGD.tmpCompValiMongo_DJTot019 b on b.num_ruc = a.num_ruc

)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivValiFVMon_DJ019 sample 100;
--select count(1) from BDDWESTGD.tmpUnivValiFVMon_DJ019; --


/*========================================================================================= */
/*********************************INSERTA EN TABLA HECHOS ***********************************/
/*========================================================================================= */

	---INSERTA VALIDOS C/S DJ PARA LA 1ERA COMPARACION (TRANS VS FVIRTUAL)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN ,MTO_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K019012022',
	    CURRENT_DATE,
	    SUM(z.mto_deduccionTran),
	    SUM(z.mto_deduccionFV)
	FROM
		(
			Select
			num_rucTra, 
			num_rucFV, 
			ind_DJ, 
			mto_deduccionTran,  
			mto_deduccionFV
			BDDWESTGD.tmpUnivValiTranFV_DJ019		
		) z
	GROUP BY 1,2,3,4
	;

	---INSERTA VALIDOS C/S DJ PARA LA 2DA COMPARACION (FVIRTUAL VS MONGODB)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN ,MTO_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K019022022',
	    CURRENT_DATE,
	    SUM(z.mto_deduccionFV),
	    SUM(z.mto_deduccionMon)
	FROM
		(
			Select 
			num_rucFV, 
			num_rucMongo, 
			ind_DJ, 
			mto_deduccionFV, 
			mto_deduccionMon
			BDDWESTGD.tmpUnivValiFVMon_DJ019
		) z
	GROUP BY 1,2,3,4
	;

	



/*========================================================================================= */
/********************************* HALLAR LAS DIFERENCIAS ***********************************/
/*========================================================================================= */	

	--Diferencias entre Transaccional y F-Virtual de los comprobantes VALIDOS
	Select
	num_rucTra,
	num_rucFV,
	mto_deduccionTran,
	mto_deduccionFV,
	ind_DJ 
	BDDWESTGD.tmpUnivValiTranFV_DJ019; 	
	

	--Diferencias entre F-Virtual y MongoDB de los comprobantes VALIDOS	
	Select 
	num_rucFV, 
	num_rucMongo, 	 
	mto_deduccionFV, 
	mto_deduccionMon,
	ind_DJ
	BDDWESTGD.tmpUnivValiFVMon_DJ019;

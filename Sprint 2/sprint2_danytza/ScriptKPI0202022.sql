/*========================================================================================= */
/************************************TRANSACCIONALES*****************************************/
/*========================================================================================= */

-- Cantidad de comprobantes válidos de gastos deducibles para el Rubro EsSalud – Trabajadores del Hogar

--UNIVERSO PARA EL ORIGEN: Servidor infp25s2 / Online: deduc_01 / BD: gastodeduc
-- DROP TABLE BDDWESTG.tmpCompVali_GDEsSaTrabHogar020;
CREATE MULTISET TABLE BDDWESTG.tmpCompVali_GDEsSaTrabHogar020 as
(
	Select a.num_ruc, count(a.num_ruc) cantidadTran
	From BDDWESTG.t8156cpgastodeduc a 	
	Where a.ann_ejercicio = '2022' 
		and a.ind_tip_gasto = '04' 
		and a.ind_estado = '1'
		and substr(a.num_partida,1,4) = '2022'
		and fec_doc >= '2022-01-01' and fec_doc <= '2023-01-31'
	group by 1
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTG.tmpCompVali_GDEsSaTrabHogar020 sample 100;
--select count(1) from BDDWESTG.tmpCompVali_GDEsSaTrabHogar020; --


/*========================================================================================= */
/**************************************F-VIRTUAL*********************************************/
/*========================================================================================= */

--DROP TABLE BDDWESTG.tmpGeneralSinPresentarDJ020;
CREATE MULTISET TABLE BDDWESTG.tmpGeneralSinPresentarDJ020 as 
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

--SELECT * FROM BDDWESTG.tmpGeneralSinPresentarDJ020 SAMPLE 100;
--SELECT COUNT(1) FROM BDDWESTG.tmpGeneralSinPresentarDJ020; --10132412

------------Presentaron DJ----------------------

--DROP TABLE BDDWESTG.tmpPresentaronDJ020;
CREATE MULTISET TABLE BDDWESTG.tmpPresentaronDJ020 as 
(
	SELECT 	num_ruc,
			MAX(num_sec) as num_sec 
	FROM BDDWESTG.t5847ctldecl 
	WHERE num_ejercicio = 2022
		AND num_formul = '0709' 
		AND ind_estado = '2'
	GROUP BY 1
)  with data no primary INDEX;


--SELECT * FROM BDDWESTG.tmpPresentaronDJ020 SAMPLE 100;
--SELECT COUNT(*) FROM BDDWESTG.tmpPresentaronDJ020; --75


------------NO presentaron DJ-------------------------

--DROP TABLE BDDWESTG.tmpNOPresentaronDJ020;
CREATE MULTISET TABLE BDDWESTG.tmpNOPresentaronDJ020 as 
(
	SELECT 	num_ruc, 
			num_sec 
	FROM BDDWESTG.tmpGeneralSinPresentarDJ020 
	WHERE num_ruc NOT IN (SELECT num_ruc FROM BDDWESTG.tmpPresentaronDJ020)
)  WITH DATA NO PRIMARY INDEX;

--SELECT * FROM BDDWESTG.tmpNOPresentaronDJ020 SAMPLE 100;
--SELECT COUNT(1) FROM BDDWESTG.tmpNOPresentaronDJ020; --10132412


----------------A.-Cuando el contribuyente aún no ha presentado su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------
-- DROP TABLE BDDWESTG.tmpCompValFVirtual_SinDJ020;
CREATE MULTISET TABLE BDDWESTG.tmpCompValFVirtual_SinDJ020 as
(
	Select a.num_ruc, count(a.num_ruc) cantidadFV0
	From BDDWESTG.t12734cas514det a 
    Inner join BDDWESTG.tmpNOPresentaronDJ020 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '04'
		and a.ind_archpers = '1'
		and a.ind_est_archper='0'
		and a.ind_est_formvir ='0'
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
	group by a.num_ruc

)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTG.tmpCompValFVirtual_SinDJ020 sample 100;
--select count(1) from BDDWESTG.tmpCompValFVirtual_SinDJ020; --


----------------B.Cuando el contribuyente ya presentó su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------
-- DROP TABLE BDDWESTG.tmpCompValFVirtual_ConDJ020;
CREATE MULTISET TABLE BDDWESTG.tmpCompValFVirtual_ConDJ020 as
(
	Select a.num_ruc, count(a.num_ruc) cantidadFV1
	From BDDWESTG.t12734cas514det a 
	Inner join BDDWESTG.tmpPresentaronDJ020 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '04'
		and a.ind_archpers = '1' 
		and a.ind_est_archper='0'
		and a.ind_est_formvir ='0'
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
	Group by a.num_ruc
)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTG.tmpCompValFVirtual_ConDJ020 sample 100;
--select count(1) from BDDWESTG.tmpCompValFVirtual_ConDJ020; --


-- DROP TABLE BDDWESTGD.tmpUnivCompValFV_DJ020;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivCompValFV_DJ020 as 
(
	Select num_ruc, cantidadFV0 cantidadFV, 0 ind_DjFV  
	from BDDWESTGD.tmpCompValFVirtual_SinDJ020 
	Union
	Select num_ruc, cantidadFV1 cantidadFV, 1 ind_DjFV 
	from BDDWESTGD.tmpCompValFVirtual_ConDJ020 
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivCompValFV_DJ020 sample 100;
--select count(1) from BDDWESTGD.tmpUnivCompValFV_DJ020; --


/*========================================================================================= */
/**************************************MONGO DB*********************************************/
/*========================================================================================= */

--Comprobantes VALIDOS Con DJ y Sin DJ en MongoDB 
-- DROP TABLE BDDWESTGD.tmpCompValiMongo_DJTot020;
CREATE MULTISET TABLE BDDWESTGD.tmpCompValiMongo_DJTot020 as 
(
	Select 
	num_ruc,	
	count(num_ruc) cnt_ValiMong
	from BDDWESTGD.t12734cas514det_mongodb
	where num_eje = '2022' 
		and num_form = '0709'
		and cod_tip_gasto = '04'
		and ind_archpers = '1'
		and ind_est_archpers = '0'
		and ind_est_formvirt ='0' 
		and fec_comprob >= '2022-01-01' AND fec_comprob <= '2023-01-31'
	group by num_ruc
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpCompValiMongo_DJTot020 sample 100;
--select count(1) from BDDWESTGD.tmpCompValiMongo_DJTot020; --


/*=======Hallar la marca con DJ y sin DJ en Transaccionales y Mongo DB==========*/

-- DROP TABLE BDDWESTGD.tmpUnivValiTranFV_DJ020;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivValiTranFV_DJ020 as --104
(
	Select
		a.num_ruc num_rucTra, 
		b.num_ruc num_rucFV,
		coalesce(b.ind_DjFV,0) ind_DJ,
		a.cantidadTran,
		coalesce(b.cantidadFV,0) cantidadFV
	BDDWESTGD.tmpCompVali_GDEsSaTrabHogar020 a 
	Left join BDDWESTGD.tmpUnivCompValFV_DJ020 b on b.num_ruc = a.num_ruc 
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivValiTranFV_DJ020 sample 100;
--select count(1) from BDDWESTGD.tmpUnivValiTranFV_DJ020; --



-- DROP TABLE BDDWESTGD.tmpUnivValiFVMon_DJ020;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivValiFVMon_DJ020 as --104
(
	Select
		a.num_ruc num_rucFV, 
		b.num_ruc num_rucMongo,
		coalesce(a.ind_DjFV,0) ind_DJ,
		a.cantidadFV,
		coalesce(b.cnt_ValiMong,0) cnt_ValiMong
	BDDWESTGD.tmpUnivCompValFV_DJ020 a
	Left join BDDWESTGD.tmpCompValiMongo_DJTot020 b on b.num_ruc = a.num_ruc

)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivValiFVMon_DJ020 sample 100;
--select count(1) from BDDWESTGD.tmpUnivValiFVMon_DJ020; --


/*========================================================================================= */
/*********************************INSERTA EN TABLA HECHOS ***********************************/
/*========================================================================================= */

	---INSERTA VALIDOS C/S DJ PARA LA 1ERA COMPARACION (TRANS VS FVIRTUAL)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K020012022',
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
			BDDWESTGD.tmpUnivValiTranFV_DJ020		
		) z
	GROUP BY 1,2,3,4
	;

	---INSERTA VALIDOS C/S DJ PARA LA 2DA COMPARACION (FVIRTUAL VS MONGODB)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN ,MTO_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K020022022',
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
			BDDWESTGD.tmpUnivValiFVMon_DJ020
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
		cantidadTran,  
		cantidadFV,
		ind_DJ
	BDDWESTGD.tmpUnivValiTranFV_DJ020; 	
	

	--Diferencias entre F-Virtual y MongoDB de los comprobantes VALIDOS	
	Select 
		num_rucFV, 
		num_rucMongo, 
		cantidadFV, 
		cnt_ValiMong,
		ind_DJ
	BDDWESTGD.tmpUnivValiFVMon_DJ020;

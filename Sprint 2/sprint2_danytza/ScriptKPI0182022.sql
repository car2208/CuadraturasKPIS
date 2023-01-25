/*========================================================================================= */
/************************************TRANSACCIONALES*****************************************/
/*========================================================================================= */

-- Cantidad de comprobantes válidos de gastos deducibles para el Rubro Alquiler Inmuebles:
-- DROP TABLE BDDWESTGD.tmpCompVali_GDAlqInm;
CREATE MULTISET TABLE BDDWESTGD.tmpCompVali_GDAlqInm as
(

	Select a.num_ruc, count(a.num_ruc) cantidad 
	From BDDWESTGD.t8156cpgastodeduc a 
	Left join BDDWESTGD.ddp b on a.num_ruc_emisor = b.ddp_numruc 
	Where a.ann_ejercicio = '2022' 
		and a.ind_tip_gasto = '01' 
		and coalesce(a.num_contrato,'') in ('','-','1')
		and substr(a.num_partida,1,4) = '2022'
		and a.ind_estado = '1'
		and fec_doc >= '2022-01-01' and fec_doc <= '2023-01-31'
	Group by 1;

)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTGD.tmpCompVali_GDAlqInm sample 100;
--select count(1) from BDDWESTGD.tmpCompVali_GDAlqInm; --

 
-- DROP TABLE BDDWESTGD.tmpCompObser_GDAlqInm;
CREATE MULTISET TABLE BDDWESTGD.tmpCompObser_GDAlqInm as
(
	Select a.num_ruc, count(a.num_ruc) cantidad
	From BDDWESTGD.t8157cpgastoobserv a 
	Left join BDDWESTGD.ddp b on a.num_ruc_emisor = b.ddp_numruc 
	Where a.ann_ejercicio = '2022' 
		and a.ind_tip_gasto = '01' 
		and fec_pago >= '2022-01-01' and fec_pago <= '2023-01-31'
	Group by 1;
)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTGD.tmpCompObser_GDAlqInm sample 100;
--select count(1) from BDDWESTGD.tmpCompObser_GDAlqInm; --


--UNIVERSO PARA EL ORIGEN: Servidor infp25s2 / Online: deduc_01 / BD: gastodeduc
-- DROP TABLE BDDWESTGD.tmpUniversoOrigen;
CREATE MULTISET TABLE BDDWESTGD.tmpUniversoOrigen as
(
	Select num_ruc, cantidad 
	from BDDWESTGD.tmpCompVali_GDAlqInm
	Union 
	Select num_ruc, cantidad
	from BDDWESTGD.tmpCompObser_GDAlqInm
)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTGD.tmpUniversoOrigen sample 100;
--select count(1) from BDDWESTGD.tmpUniversoOrigen; --

/*========================================================================================= */
/**************************************F-VIRTUAL*********************************************/
/*========================================================================================= */

--DROP TABLE BDDWESTGD.tmpGeneralSinPresentarDJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpGeneralSinPresentarDJ018 as 
(
	SELECT 
		num_ruc,
		MAX(num_sec) as num_sec
	FROM BDDWESTGD.t5847ctldecl 
	WHERE num_ejercicio = 2022
		AND num_formul = '0709' 
		AND ind_actual = '1' 
		AND ind_estado = '0' 
		AND ind_proceso = '1'
	GROUP BY 1
) with data no primary INDEX;

--SELECT * FROM BDDWESTGD.tmpGeneralSinPresentarDJ018 SAMPLE 100;
--SELECT COUNT(1) FROM BDDWESTGD.tmpGeneralSinPresentarDJ018; --10132412

------------Presentaron DJ----------------------

--DROP TABLE BDDWESTGD.tmpPresentaronDJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpPresentaronDJ018 as 
(
	SELECT 	num_ruc,
			MAX(num_sec) as num_sec 
	FROM BDDWESTGD.t5847ctldecl 
	WHERE num_ejercicio = 2022
		AND num_formul = '0709' 
		AND ind_estado = '2'
	GROUP BY 1
)  with data no primary INDEX;


--SELECT * FROM BDDWESTGD.tmpPresentaronDJ018 SAMPLE 100;
--SELECT COUNT(*) FROM BDDWESTGD.tmpPresentaronDJ018; --75


------------NO presentaron DJ-------------------------

--DROP TABLE BDDWESTGD.tmpNOPresentaronDJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpNOPresentaronDJ018 as 
(
	SELECT 	num_ruc, 
			num_sec 
	FROM BDDWESTGD.tmpGeneralSinPresentarDJ018 
	WHERE num_ruc NOT IN (SELECT num_ruc FROM BDDWESTGD.tmpPresentaronDJ018)
)  WITH DATA NO PRIMARY INDEX;

--SELECT * FROM BDDWESTGD.tmpNOPresentaronDJ018 SAMPLE 100;
--SELECT COUNT(1) FROM BDDWESTGD.tmpNOPresentaronDJ018; --10132412


----------------A.-Cuando el contribuyente aún no ha presentado su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------
-- DROP TABLE BDDWESTGD.tmpCompValFVirtual_SinDJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpCompValFVirtual_SinDJ018 as
(
	Select a.num_ruc, count(a.num_ruc) cnt_valfv0
	From BDDWESTGD.t12734cas514det a 
    Inner join BDDWESTGD.tmpNOPresentaronDJ018 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '01'
		and a.ind_archpers = '1'  
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
	group by a.num_ruc

)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTGD.tmpCompValFVirtual_SinDJ018 sample 100;
--select count(1) from BDDWESTGD.tmpCompValFVirtual_SinDJ018; --


----------------COMPROBANTES OBSERVADOS----------------
-- DROP TABLE BDDWESTGD.tmpCompObsFVirtual_SinDJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpCompObsFVirtual_SinDJ018 as
(
	Select a.num_ruc, count(a.num_ruc) cnt_obsfv0
	From BDDWESTGD.t12734cas514det a 
	Inner join BDDWESTGD.tmpNOPresentaronDJ018 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '01'
		and a.ind_archpers = '1'  
		and a.ind_est_archpers = '0'
		and a.ind_est_formvir= '0'
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
		and a.des_inconsistencia<> ' '
	group by a.num_ruc

)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTGD.tmpCompObsFVirtual_SinDJ018 sample 100;
--select count(1) from BDDWESTGD.tmpCompObsFVirtual_SinDJ018; --



----------------B.Cuando el contribuyente ya presentó su DDJJ Anual------------------------

----------------COMPROBANTES VÁLIDOS----------------
-- DROP TABLE BDDWESTGD.tmpCompValFVirtual_ConDJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpCompValFVirtual_ConDJ018 as
(
	Select a.num_ruc, count(a.num_ruc) cnt_valfv1
	From BDDWESTGD.t12734cas514det a 
	Inner join BDDWESTGD.tmpPresentaronDJ018 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '01'
		and a.ind_archpers = '1'  
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
	Group by a.num_ruc
)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTGD.tmpCompValFVirtual_ConDJ018 sample 100;
--select count(1) from BDDWESTGD.tmpCompValFVirtual_ConDJ018; --


----------------COMPROBANTES OBSERVADOS----------------
-- DROP TABLE BDDWESTGD.tmpCompObsFVirtual_ConDJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpCompObsFVirtual_ConDJ018 as
(	
	Select a.num_ruc, count(a.num_ruc) cnt_obsfv1
	From BDDWESTGD.t12734cas514det a 
	Inner join BDDWESTGD.tmpPresentaronDJ018 b on a.num_sec = b.num_sec
	Where a.cod_tip_gasto = '01'
		and a.ind_archpers = '1'  
		and a.ind_est_archpers = '0'
		and a.ind_est_formvir= '0'
		and a.fec_comprob >= '2022-01-01' and a.fec_comprob <= '2023-01-31'
		and a.des_inconsistencia <> ' '
	Group by a.num_ruc
)
WITH DATA NO PRIMARY INDEX;

--select * from BDDWESTGD.tmpCompObsFVirtual_ConDJ018 sample 100;
--select count(1) from BDDWESTGD.tmpCompObsFVirtual_ConDJ018; --

-- DROP TABLE BDDWESTGD.tmpUnivCompValFV_DJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivCompValFV_DJ018 as 
(
	Select num_ruc, cnt_valfv0 cnt_valfv, 0 ind_DjFV  
	from BDDWESTGD.tmpCompValFVirtual_SinDJ018 
	Union
	Select num_ruc, cnt_valfv1 cnt_valfv, 1 ind_DjFV 
	from BDDWESTGD.tmpCompValFVirtual_ConDJ018 
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivCompValFV_DJ018 sample 100;
--select count(1) from BDDWESTGD.tmpUnivCompValFV_DJ018; --


-- DROP TABLE BDDWESTGD.tmpUnivCompObsFV_DJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivCompObsFV_DJ018 as 
(
	Select num_ruc, cnt_obsfv0 cnt_obsfv, 0 ind_DjFV 
	from BDDWESTGD.tmpCompObsFVirtual_SinDJ018 
	Union
	Select num_ruc, cnt_obsfv1 cnt_obsfv, 1 ind_DjFV 
	from BDDWESTGD.tmpCompObsFVirtual_ConDJ018 
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivCompObsFV_DJ018 sample 100;
--select count(1) from BDDWESTGD.tmpUnivCompObsFV_DJ018; --


/*========================================================================================= */
/**************************************MONGO DB*********************************************/
/*========================================================================================= */
--Comprobantes VALIDOS Con DJ y Sin DJ en MongoDB 
-- DROP TABLE BDDWESTGD.tmpCompValiMongo_DJTot018;
CREATE MULTISET TABLE BDDWESTGD.tmpCompValiMongo_DJTot018 as 
(
	Select 
	num_ruc,	
	count(num_ruc) cnt_ValiMong
	from BDDWESTGD.t12734cas514det_mongodb
	where num_eje = '2022' 
		and num_form = '0709'
		and cod_tip_gasto = '01'
		and ind_archpers = '1'  
		and fec_comprob >= '2022-01-01' AND fec_comprob <= '2023-01-31'
	group by num_ruc
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpCompValiMongo_DJTot018 sample 100;
--select count(1) from BDDWESTGD.tmpCompValiMongo_DJTot018; --


--Comprobantes OBSERVADOS Con DJ y Sin DJ en MongoDB 
-- DROP TABLE BDDWESTGD.tmpCompObseMongo_DJTot018;
CREATE MULTISET TABLE BDDWESTGD.tmpCompObseMongo_DJTot018 as 
(
	Select 
	num_ruc,	
	count(num_ruc) cnt_ObseMong
	from BDDWESTGD.t12734cas514det_mongodb
	where num_eje = '2022' 
		and num_form = '0709'	
		and cod_tip_gasto = '01'
		and ind_archpers = '1'
		and ind_est_archpers = '0'
		and ind_est_formvirt= '0'
		and fec_comprob >= '2022-01-01' AND fec_comprob <= '2023-01-31'
		and des_inconsistencia <> ' '
	group by num_ruc
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpCompObseMongo_DJTot018 sample 100;
--select count(1) from BDDWESTGD.tmpCompObseMongo_DJTot018; --


/*=======Hallar la marca con DJ y sin DJ en Transaccionales==========*/
-- DROP TABLE BDDWESTGD.tmpUnivValiTranFV_DJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivValiTranFV_DJ018 as --104
(
	Select
		a.num_ruc num_rucTra, 
		b.num_ruc num_rucFV,
		coalesce(b.ind_DjFV,0) ind_DJ,
		a.cantidad,
		coalesce(b.cnt_valfv,0) cnt_valfv
	BDDWESTGD.tmpUniversoOrigen a
	Left join BDDWESTGD.tmpUnivCompValFV_DJ018 b on b.num_ruc = a.num_ruc
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivValiTranFV_DJ018 sample 100;
--select count(1) from BDDWESTGD.tmpUnivValiTranFV_DJ018; --



-- DROP TABLE BDDWESTGD.tmpUnivObseTranFV_DJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivObseTranFV_DJ018 as --104
(
	Select
		a.num_ruc num_rucTra, 
		b.num_ruc num_rucFV,
		coalesce(b.ind_DjFV,0) ind_DJ,
		a.cantidad,
		coalesce(b.cnt_obsfv,0) cnt_obsfv
	BDDWESTGD.tmpUniversoOrigen a
	Left join BDDWESTGD.tmpUnivCompObsFV_DJ018 b on b.num_ruc = a.num_ruc
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivObseTranFV_DJ018 sample 100;
--select count(1) from BDDWESTGD.tmpUnivObseTranFV_DJ018; --



/*=======Hallar la marca con DJ y sin DJ en MongoDB==========*/

---Hallar la marca con DJ y sin DJ en VALIDOS
-- DROP TABLE BDDWESTGD.tmpUnivValiFVMon_DJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivValiFVMon_DJ018 as --104
(
	Select
		a.num_ruc num_rucFV, 
		b.num_ruc num_rucMongo,
		coalesce(a.ind_DjFV,0) ind_DJ,
		a.cnt_valfv,
		coalesce(b.cnt_ValiMong,0) cnt_ValiMong
	BDDWESTGD.tmpUnivCompValFV_DJ018 a
	Left join BDDWESTGD.tmpCompValiMongo_DJTot018 b on b.num_ruc = a.num_ruc

)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivValiFVMon_DJ018 sample 100;
--select count(1) from BDDWESTGD.tmpUnivValiFVMon_DJ018; --



---Hallar la marca con DJ y sin DJ en OBSERVADOS
-- DROP TABLE BDDWESTGD.tmpUnivObseFVMon_DJ018;
CREATE MULTISET TABLE BDDWESTGD.tmpUnivObseFVMon_DJ018 as --104
(
	Select
		a.num_ruc num_rucFV, 
		b.num_ruc num_rucMongo,
		coalesce(a.ind_DjFV,0) ind_DJ,
		a.cnt_obsfv,
		coalesce(b.cnt_ObseMong,0) cnt_ObseMong	
	BDDWESTGD.tmpUnivCompObsFV_DJ018 a
	Left join BDDWESTGD.tmpCompObseMongo_DJTot018 b on b.num_ruc = a.num_ruc
)
WITH DATA NO PRIMARY INDEX;
--select * from BDDWESTGD.tmpUnivObseFVMon_DJ018 sample 100;
--select count(1) from BDDWESTGD.tmpUnivObseFVMon_DJ018; --


/*========================================================================================= */
/*********************************INSERTA EN TABLA HECHOS ***********************************/
/*========================================================================================= */

	---INSERTA VALIDOS C/S DJ PARA LA 1ERA COMPARACION (TRANS VS FVIRTUAL)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K018012022',
	    CURRENT_DATE,
	    SUM(z.cantidad),
	    SUM(z.cnt_valfv)
	FROM
		(
			Select
			num_rucTra, 
			num_rucFV, 
			ind_DJ, 
			cantidad,  
			cnt_valfv
			BDDWESTGD.tmpUnivValiTranFV_DJ018 			
		) z
	GROUP BY 1,2,3,4
	;

	---INSERTA VALIDOS C/S DJ PARA LA 2DA COMPARACION (FVIRTUAL VS MONGODB)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K018032022',
	    CURRENT_DATE,
	    SUM(z.cnt_valfv),
	    SUM(z.cnt_ValiMong)
	FROM
		(
			Select 
			num_rucFV, 
			num_rucMongo, 
			ind_DJ, 
			cnt_valfv, 
			cnt_ValiMong
			BDDWESTGD.tmpUnivValiFVMon_DJ018
		) z
	GROUP BY 1,2,3,4
	;

	---INSERTA OBSERVADOS C/S DJ PARA LA 1ERA COMPARACION (TRANS VS FVIRTUAL)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K018022022',
	    CURRENT_DATE,
	    SUM(z.cantidad),
	    SUM(z.cnt_obsfv)
	FROM
		(
			Select 
			num_rucTra, 
			num_rucFV, 
			ind_DJ, 
			cantidad, 
			cnt_obsfv
			BDDWESTGD.tmpUnivObseTranFV_DJ018
		) z
	GROUP BY 1,2,3,4
	;

	---INSERTA OBSERVADOS C/S DJ PARA LA 2DA COMPARACION (FVIRTUAL VS MONGODB)
	INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 	
		'2023',
		z.ind_DJ,
	    'K018042022',
	    CURRENT_DATE,
	    SUM(z.cnt_obsfv),
	    SUM(z.cnt_ObseMong)
	FROM
		(
			Select 
			num_rucFV, 
			num_rucMongo, 
			ind_DJ, 
			cnt_obsfv, 
			cnt_ObseMong
			BDDWESTGD.tmpUnivObseFVMon_DJ018
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
	cantidad,
	cnt_valfv,
	ind_DJ
	BDDWESTGD.tmpUnivValiTranFV_DJ018; 	


	--Diferencias entre F-Virtual y MongoDB de los comprobantes VALIDOS	
	Select 
	num_rucFV, 
	num_rucMongo, 	 
	cnt_valfv, 
	cnt_ValiMong,
	ind_DJ
	BDDWESTGD.tmpUnivValiFVMon_DJ018;


	--Diferencias entre Transaccional y F-Virtual de los comprobantes OBSERVADOS
	Select 
	num_rucTra, 
	num_rucFV, 	 
	cantidad, 
	cnt_obsfv,
	ind_DJ
	BDDWESTGD.tmpUnivObseTranFV_DJ018;


	--Diferencias entre F-Virtual y MongoDB de los comprobantes OBSERVADOS
	Select 
	num_rucFV, 
	num_rucMongo, 
	cnt_obsfv, 
	cnt_ObseMong,
	ind_DJ
	BDDWESTGD.tmpUnivObseFVMon_DJ018;
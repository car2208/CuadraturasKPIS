/****************************TRANSACCIONAL*****************************************/
------------Genera Detalle Transaccional-------------------------------

DROP TABLE BDDWESTGD.tmp093168_kpigr14_detcpeobs_tr;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_detcpeobs_tr
AS
(
SELECT 
	DISTINCT 
	TRIM(x0.num_ruc) as num_ruc,
	coalesce(x2.ind_presdj,0) as ind_presdj,
	x0.ann_ejercicio,
	TRIM(x0.num_ruc_emisor) as num_ruc_emisor,
	TRIM(x0.cod_tip_doc) as cod_tip_doc,
	TRIM(x0.ser_doc) as ser_doc,
	TRIM(x0.num_doc) as num_doc
FROM bddwestgd.t8157cpgastoobserv x0
LEFT JOIN bddwestgd.ddp x1 ON x0.num_ruc_emisor = x1.ddp_numruc
LEFT JOIN bddwestgd.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '05' 
AND x0.fec_pago >= DATE '2022-01-01' AND fec_pago <= DATE '2022-12-31'
AND x0.ind_inconsistencia <> 'I1'
)
WITH DATA NO PRIMARY INDEX;

/***************************FVIRTUAL**************************************************/

------ Genera Detalle de COMPROBANTES OBSERVADOS en FVIRTUAL-------------------

DROP TABLE BDDWESTGD.tmp093168_kpigr14_detcpeobs_fv;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_detcpeobs_fv
AS
(
	SELECT
		DISTINCT 
        extract(year from fec_comprob) as ann_ejercicio,
		TRIM(a.num_ruc) as num_ruc,
		b.ind_presdj,
		TRIM(a.num_doc_emisor) as num_doc_emisor,
		TRIM(a.cod_tip_comprob) as cod_tip_comprob ,
		TRIM(a.num_serie) as num_serie,
		TRIM(a.num_comprob)  as num_comprob
	FROM BDDWESTGD.t12734cas514det a
	INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
	WHERE a.cod_tip_gasto = '05'
	AND a.ind_est_archpers = '0'  -- OBSERVADO
	AND a.ind_inconsistencia <> 'I1'
	AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
	AND a.des_inconsistencia <> ' '
) WITH DATA NO PRIMARY INDEX;


/************************MONGO DB*****************************************************/

------ Genera Detalle de COMPROBANTES OBSERVADOS en MONGODB-------------------

DROP TABLE BDDWESTGD.tmp093168_kpigr14_detcpeobs_mdb;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_detcpeobs_mdb
AS
(
SELECT DISTINCT 
        extract(year from fec_comprob) as ann_ejercicio,
		TRIM(a.num_ruc) as num_ruc,
		b.ind_presdj,
		TRIM(a.num_doc_emisor) as num_doc_emisor,
		TRIM(a.cod_tip_comprob) as cod_tip_comprob ,
		TRIM(a.num_serie) as num_serie,
		TRIM(a.num_comprob)  as num_comprob
FROM BDDWESTGD.t12734cas514det_mongodb a
INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers = '0'  -- OBSERVADO
AND a.ind_inconsistencia <> 'I1'
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
AND a.des_inconsistencia <> ' '
) WITH DATA NO PRIMARY INDEX;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTGD.tmp093168_kpigr14_obs_cnorigen;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_obs_cnorigen AS
(
	SELECT ind_presdj,count(num_doc) as cant_comp_origen
	FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_tr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE BDDWESTGD.tmp093168_kpigr14_obs_cndestino1;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_obs_cndestino1 AS
(
	SELECT ind_presdj,count(num_comprob) as cant_comp_destino1
	FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_fv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE BDDWESTGD.tmp093168_kpigr14_obs_cndestino2	;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_obs_cndestino2 AS
(
	SELECT ind_presdj,count(num_comprob) as cant_comp_destino2
	FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_mdb
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/********************INSERT EN TABLA FINAL***********************************/
--'K014032022'
--'K014042022'
	DELETE FROM BDDWESTGD.T11908DETKPITRIBINT 
	WHERE COD_KPI='K014032022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTGD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  '2022',
	        z.ind_presdj,
	       'K014032022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT
			       x0.ind_presdj,
			       x0.cant_comp_origen as cant_origen,
			       coalesce(x1.cant_comp_destino1,0) as cant_destino
			FROM BDDWESTGD.tmp093168_kpigr14_obs_cnorigen x0
			LEFT JOIN BDDWESTGD.tmp093168_kpigr14_obs_cndestino1 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4;


	DELETE FROM BDDWESTGD.T11908DETKPITRIBINT 
	WHERE COD_KPI='K014042022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTGD.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  '2022' as cod_kpi,
	        z.ind_presdj,
	        'K014042022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT x0.ind_presdj,
			       x0.cant_comp_destino1 AS cant_origen,
			       coalesce(x1.cant_comp_destino2,0) AS cant_destino
			FROM BDDWESTGD.tmp093168_kpigr14_obs_cndestino1 x0
			LEFT JOIN BDDWESTGD.tmp093168_kpigr14_obs_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

/******************************************************************************/

	DROP TABLE BDDWESTGD.DIF_K014032022;
    CREATE MULTISET TABLE BDDWESTGD.DIF_K014032022 AS (
	SELECT DISTINCT 
			'K014032022' cod_kpi,
			y0.ann_ejercicio,
			y0.num_ruc,
			y0.ind_presdj,
			y0.num_ruc_emisor,
			y0.cod_tip_doc,
			y0.ser_doc,
			y0.num_doc
	

	FROM (
		SELECT 		ann_ejercicio,
					num_ruc,
					ind_presdj,
					num_ruc_emisor,
					cod_tip_doc,
					ser_doc,
					num_doc
		FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_tr
		EXCEPT ALL
		SELECT  ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob
		FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_fv
	) y0
	) WITH DATA NO PRIMARY INDEX;


	DROP TABLE BDDWESTGD.DIF_K014042022;
    CREATE MULTISET TABLE BDDWESTGD.DIF_K014042022 AS (
	SELECT DISTINCT 
			'K014042022' cod_kpi,
			y0.ann_ejercicio,
			y0.num_ruc,
			y0.ind_presdj,
			y0.num_doc_emisor,
			y0.cod_tip_comprob,
			y0.num_serie,
			y0.num_comprob
	
	FROM (
	    SELECT  
	        ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob
		FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_fv
		EXCEPT ALL
		SELECT  ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob
		FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_mdb
	) y0
	) WITH DATA NO PRIMARY INDEX;

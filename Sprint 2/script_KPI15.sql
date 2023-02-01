/**************************************************************************************************/
----15
------------Genera Detalle Transaccional-------------------------------


CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi15_detcpeval_tr
AS
(
SELECT 
	DISTINCT
	TRIM(x0.num_ruc) as num_ruc, 
	coalesce(x2.ind_presdj,0) as ind_presdj,
	x0.ann_ejercicio,
	TRIM(x0.num_ruc_emisor) as num_ruc_emisor
	TRIM(x0.cod_tip_doc) as cod_tip_doc,
	TRIM(x0.ser_doc) as ser_doc,
	TRIM(x0.num_doc) as num_doc,
	x0.mto_doc_fin_mn,
	x0.mto_deduccion_fin
FROM bddwestgd.t8156cpgastodeduc x0 
LEFT JOIN bddwestgd.ddp x1 ON x0.num_ruc_emisor = x1.ddp_numruc 
LEFT JOIN bddwestgd.tmp093168_kpiperindj x2 on x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '05' 
AND x0.ind_estado = '1'
AND x0.fec_doc >= DATE '2022-01-01' AND fec_doc <= DATE '2022-12-31'
)
WITH DATA NO PRIMARY INDEX;

-------Genera Detalle Fvirtual-------------------
-------COMPROBANTES VÁLIDOS------------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi15_detcpeval_fv
AS
(
SELECT 	DISTINCT 
        extract(year from fec_comprob) as ann_ejercicio,
		TRIM(a.num_ruc) as num_ruc,
		b.ind_presdj,
		TRIM(a.num_doc_emisor) as num_doc_emisor,
		TRIM(a.cod_tip_comprob) as cod_tip_comprob ,
		TRIM(a.num_serie) as num_serie,
		TRIM(a.num_comprob)  as num_comprob,
		a.mto_comprob,
		a.mto_deduccion
FROM BDDWESTGD.t12734cas514det a 
INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
) WITH DATA NO PRIMARY INDEX;



-------Genera Detalle MongoDB-------------------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi15_detcpeval_mdb
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
		TRIM(a.num_comprob)  as num_comprob,
		a.mto_comprob,
		a.mto_deduccion
FROM BDDWESTGD.t12734cas514det_mongodb a 
INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
) WITH DATA NO PRIMARY INDEX;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTGD.tmp093168_kpigr15_cnorigen;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr15_cnorigen AS
(
	SELECT ind_presdj,sum(mto_deduccion_fin) as mto_deduc_origen
	FROM BDDWESTGD.tmp093168_kpi15_detcpeval_tr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE BDDWESTGD.tmp093168_kpigr15_cndestino1;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr15_cndestino1 AS
(
	SELECT ind_presdj,sum(mto_deduccion) as mto_deduc_destino1
	FROM BDDWESTGD.tmp093168_kpi15_detcpeval_fv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE BDDWESTGD.tmp093168_kpigr15_cndestino2	;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr15_cndestino2 AS
(
	SELECT ind_presdj,sum(mto_deduccion) as mto_deduc_destino2
	FROM BDDWESTGD.tmp093168_kpi15_detcpeval_mdb
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/********************INSERT EN TABLA FINAL***********************************/
--'K015012022'
--'K015022022'
	DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
	WHERE COD_KPI='K015012022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  '2022',
	        z.ind_presdj,
	       'K015012022',
	        CURRENT_DATE,
	        SUM(z.mto_origen),
	        SUM(z.mto_destino)
	FROM
		(
			SELECT
			       x0.ind_presdj,
			       x0.mto_deduc_origen as mto_origen,
			       coalesce(x1.mto_deduc_destino1,0) as mto_destino
			FROM BDDWESTGD.tmp093168_kpigr15_cnorigen x0
			LEFT JOIN BDDWESTGD.tmp093168_kpigr15_cndestino1 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;


	DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
	WHERE COD_KPI='K015022022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  '2022',
	        z.ind_presdj,
	        'K015022022',
	        CURRENT_DATE,
	        SUM(z.mto_origen),
	        SUM(z.mto_destino)
	FROM
		(
			SELECT x0.ind_presdj,
			       x0.mto_deduc_destino1 AS mto_origen,
			       coalesce(x1.mto_deduc_destino2,0) AS mto_destino
			FROM BDDWESTG.tmp093168_kpigr15_cndestino1 x0
			LEFT JOIN BDDWESTG.tmp093168_kpigr15_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

/******************************************************************************/


	SELECT DISTINCT 
			'K015012022' cod_kpi,
			y0.ann_ejercicio,
			y0.num_ruc,
			y0.ind_presdj,
			y0.num_ruc_emisor,
			y0.cod_tip_doc,
			y0.ser_doc,
			y0.num_doc,
			y0.mto_deduccion_fin
	

	FROM (
		SELECT 		ann_ejercicio,
					num_ruc,
					ind_presdj,
					num_ruc_emisor,
					cod_tip_doc,
					ser_doc,
					num_doc,
					mto_deduccion_fin
		FROM BDDWESTG.tmp093168_kpigr15_detcpeval_tr
		EXCEPT ALL
		SELECT  ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob,
			mto_deduccion
		FROM BDDWESTG.tmp093168_kpigr15_detcpeval_fv
	) y0
	;


	SELECT DISTINCT 
			'K015022022' cod_kpi,
			y0.ann_ejercicio,
			y0.num_ruc,
			y0.ind_presdj,
			y0.num_doc_emisor,
			y0.cod_tip_comprob,
			y0.num_serie,
			y0.num_comprob
			y0.mto_deduccion
	
	FROM (
	    SELECT  
	        ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob,
			mto_deduccion
		FROM BDDWESTG.tmp093168_kpigr15_detcpeval_fv
		EXCEPT ALL
		SELECT  ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob,
			mto_deduccion
		FROM BDDWESTG.tmp093168_kpigr15_detcpeval_mdb
	) y0

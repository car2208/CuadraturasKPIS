/******************************************************************************************************/
----16
------------Genera Detalle Transaccional-------------------------------


CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi16_detcpeobs_tr
AS
(
SELECT  
    x0.num_ruc,
    coalesce(x2.ind_presdj,0) as ind_presdj,
	x0.ann_ejercicio,
	x0.num_ruc_emisor,
	x0.cod_tip_doc,
	x0.ser_doc,
	x0.num_doc
FROM bddwestgd.t8157cpgastoobserv x0
LEFT JOIN ddp x0 ON x0.num_ruc_emisor = x1.ddp_numruc 
LEFT JOIN bddwestgd.tmp093168_kpiperindj x2 on x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '03' 
AND x0.fec_pago >=  DATE '2022-01-01' AND x0.fec_pago <=  DATE '2022-12-31'
) WITH DATA NO PRIMARY INDEX;

----------Genera Detalle FVirtual --------------------------------------


--------COMPROBANTES OBSERVADOS--------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi16_detcpeobs_fv
AS
(
SELECT a.num_ruc,b.ind_presdj count(a.num_ruc) cantidad
FROM bddwestgd.t12734cas514det a 
INNER JOIN bddwestgd.tmp093168_kpiperindj b ON a.num_sec = b.num_sec
WHERE a.cod_tip_gasto = '03'
AND a.ind_archpers = '1'  
AND a.ind_est_archpers = '0'
AND a.ind_est_formvir= '0'
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
AND a.des_inconsistencia!= ' '
group by 1,2
) WITH DATA NO PRIMARY INDEX;


---------------Genera Detalle MongoDB--------------------



--------COMPROBANTES OBSERVADOS--------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi16_detcpeobs_mdb
AS
(
SELECT a.num_ruc,b.ind_presdj count(a.num_ruc) cantidad
FROM bddwestgd.t12734cas514det_mongodb a 
INNER JOIN bddwestgd.tmp093168_kpiperindj b ON a.num_sec = b.num_sec
WHERE a.cod_tip_gasto = '03'
AND a.ind_archpers = '1'  
AND a.ind_est_archpers = '0'
AND a.ind_est_formvir= '0'
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
AND a.des_inconsistencia!= ' '
group by 1,2
) WITH DATA NO PRIMARY INDEX;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTGD.tmp093168_kpigr14_cnorigen;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_cnorigen AS
(
	SELECT ind_presdj,count(num_doc) as cant_comp_origen
	FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_tr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE BDDWESTGD.tmp093168_kpigr14_cndestino1;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_cndestino1 AS
(
	SELECT ind_presdj,count(num_comprob) as cant_comp_destino1
	FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_fv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE BDDWESTGD.tmp093168_kpigr14_cndestino2	;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr14_cndestino2 AS
(
	SELECT ind_presdj,count(num_comprob) as cant_comp_destino2
	FROM BDDWESTGD.tmp093168_kpigr14_detcpeobs_mdb
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/********************INSERT EN TABLA FINAL***********************************/
--'K014032022'
--'K014042022'
	DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
	WHERE COD_KPI='K016032022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  '2022',
	        z.ind_presdj,
	       'K016032022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT
			       x0.ind_presdj,
			       x0.cant_comp_origen as cant_origen,
			       coalesce(x1.cant_comp_destino1,0) as cant_destino
			FROM BDDWESTGD.tmp093168_kpigr14_cnorigen x0
			LEFT JOIN BDDWESTGD.tmp093168_kpigr14_cndestino1 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;


	DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
	WHERE COD_KPI='K016042022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  '2022',
	        z.ind_presdj,
	        'K016042022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT x0.ind_presdj,
			       x0.cant_comp_destino1 AS cant_origen,
			       coalesce(x1.cant_comp_destino2,0) AS cant_destino
			FROM BDDWESTG.tmp093168_kpigr14_cndestino1 x0
			LEFT JOIN BDDWESTG.tmp093168_kpigr14_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

/******************************************************************************/


	SELECT DISTINCT 
			'K016032022' cod_kpi,
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
		FROM BDDWESTG.tmp093168_kpigr14_detcpeobs_tr
		EXCEPT ALL
		SELECT  ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob
		FROM BDDWESTG.tmp093168_kpigr14_detcpeobs_fv
	) y0
	;


	SELECT DISTINCT 
			'K016042022' cod_kpi,
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
		FROM BDDWESTG.tmp093168_kpigr14_detcpeobs_fv
		EXCEPT ALL
		SELECT  ann_ejercicio,
			num_ruc,
			ind_presdj,
			num_doc_emisor,
			cod_tip_comprob,
			num_serie,
			num_comprob
		FROM BDDWESTG.tmp093168_kpigr14_detcpeobs_mdb
	) y0

/******************************************************************************************************/
----16
------------Genera Detalle Transaccional-------------------------------


CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi16_detcpeval_tr
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
FROM bddwestgd.t8156cpgastodeduc x0
LEFT JOIN bddwestgd.ddp x1 ON x0.num_ruc_emisor = x1.ddp_numruc 
LEFT JOIN bddwestgd.tmp093168_kpiperindj x2 on x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '03' 
AND x0.ind_estado = '1'
AND x0.fec_doc >= DATE '2022-01-01' AND x0.fec_doc <= DATE '2022-12-31'
) WITH DATA NO PRIMARY INDEX;


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

-------COMPROBANTES VÁLIDOS------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi16_detcpeval_fv
AS
(
SELECT a.num_ruc,b.ind_presdj,count(a.num_ruc) cantidad 
FROM bddwestgd.t12734cas514det a 
INNER JOIN bddwestgd.tmp093168_kpiperindj b ON a.num_sec = b.num_sec
WHERE a.cod_tip_gasto = '03'
AND a.ind_archpers = '1'  
AND a.ind_est_archpers = '0'
AND a.ind_est_formvir= '0'
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
group by 1,2
) WITH DATA NO PRIMARY INDEX;


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


-------COMPROBANTES VÁLIDOS------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi16_detcpeval_mdb
AS
(
SELECT a.num_ruc,b.ind_presdj,count(a.num_ruc) cantidad 
FROM bddwestgd.t12734cas514det_mongodb a 
INNER JOIN bddwestgd.tmp093168_kpiperindj b ON a.num_sec = b.num_sec
WHERE a.cod_tip_gasto = '03'
AND a.ind_archpers = '1'  
AND a.ind_est_archpers = '0'
AND a.ind_est_formvir= '0'
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
group by 1,2
) WITH DATA NO PRIMARY INDEX;


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

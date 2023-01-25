/****************************TRANSACCIONAL*****************************************/
------------Genera Detalle Transaccional-------------------------------
----14

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi14_detcpeval_tr
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
LEFT JOIN bddwestgd.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '05' 
AND x0.ind_estado = '1'
AND x0.fec_doc >= DATE '2022-01-01' AND x0.fec_doc <= DATE '2022-12-31'
)
WITH DATA NO PRIMARY INDEX;


CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi14_detcpeobs_tr
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
LEFT JOIN bddwestgd.ddp x1 ON x0.num_ruc_emisor = x1.ddp_numruc
LEFT JOIN bddwestgd.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '05' 
AND x0.fec_pago >= DATE '2022-01-01' AND fec_pago <= DATE '2022-12-31'
AND x0.ind_inconsistencia != 'I1'
)
WITH DATA NO PRIMARY INDEX;

/***************************FVIRTUAL**************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en FVIRTUAL----------------------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi14_detcpeval_fv
AS
(
SELECT a.num_ruc,b.ind_presdj,count(a.num_ruc) cantidad 
FROM BDDWESTGD.t12734cas514det a
INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
AND a.fec_comprob >= DATE '2022-01-01'AND a.fec_comprob <=  DATE '2022-12-31'
group by 1,2
) WITH DATA NO PRIMARY INDEX;

------ Genera Detalle de COMPROBANTES OBSERVADOS en FVIRTUAL-------------------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi14_detcpeobs_fv
AS
(
SELECT a.num_ruc,b.ind_presdj,count(a.num_ruc) cantidad 
FROM BDDWESTGD.t12734cas514det a
INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers = '0'  -- OBSERVADO
AND a.ind_inconsistencia != 'I1'
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
AND a.des_inconsistencia!= ' '
group by 1,2
) WITH DATA NO PRIMARY INDEX;


/************************MONGO DB*****************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en MONGODB----------------------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi14_detcpeval_mdb
AS
(
SELECT a.num_ruc,b.ind_presdj,count(a.num_ruc) cantidad 
FROM BDDWESTGD.t12734cas514det_mongodb a
INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
AND a.fec_comprob >= DATE '2022-01-01'AND a.fec_comprob <=  DATE '2022-12-31'
group by 1,2
) WITH DATA NO PRIMARY INDEX;

------ Genera Detalle de COMPROBANTES OBSERVADOS en MONGODB-------------------

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi14_detcpeobs_mdb
AS
(
SELECT a.num_ruc,b.ind_presdj,count(a.num_ruc) cantidad 
FROM BDDWESTGD.t12734cas514det_mongodb a
INNER JOIN BDDWESTGD.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers = '0'  -- OBSERVADO
AND a.ind_inconsistencia != 'I1'
AND a.fec_comprob >= DATE '2022-01-01' AND a.fec_comprob <= DATE '2022-12-31'
AND a.des_inconsistencia!= ' '
group by 1,2
) WITH DATA NO PRIMARY INDEX;


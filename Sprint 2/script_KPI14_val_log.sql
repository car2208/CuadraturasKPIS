/*
DROP TABLE bddwestg.tmp093168_kpi14_detcpeval_tr;
DROP TABLE bddwestg.tmp093168_kpi14_detcpeval_fv;
DROP TABLE bddwestg.tmp093168_kpi14_detcpeval_mdb;
DROP TABLE bddwestg.tmp093168_kpigr14_val_cnorigen;
DROP TABLE bddwestg.tmp093168_kpigr14_val_cndestino1;
DROP TABLE bddwestg.tmp093168_kpigr14_val_cndestino2  ;
DROP TABLE bddwestg.tmp093168_dif_K014012022;
DROP TABLE bddwestg.tmp093168_dif_K014022022;
*/

/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

------------Genera Detalle Transaccional Comprobantes Válidos-------------------------------


CREATE MULTISET TABLE bddwestg.tmp093168_kpi14_detcpeval_tr
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
FROM bddwestg.t8156cpgastodeduc x0 
LEFT JOIN bddwestg.ddp x1 ON x0.num_ruc_emisor = x1.ddp_numruc
LEFT JOIN bddwestg.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '05' 
AND x0.ind_estado = '1'
AND x0.fec_doc >= CAST('2022-01-01' AS DATE FORMAT 'YYYY-MM-DD') 
AND x0.fec_doc <= CAST('2022-12-31' AS DATE FORMAT 'YYYY-MM-DD')
)
WITH DATA NO PRIMARY INDEX;


/***************************FVIRTUAL**************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en FVIRTUAL----------------------


CREATE MULTISET TABLE bddwestg.tmp093168_kpi14_detcpeval_fv
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
FROM bddwestg.t12734cas514det a
INNER JOIN bddwestg.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
AND a.fec_comprob >= CAST('2022-01-01' AS DATE FORMAT 'YYYY-MM-DD')
AND a.fec_comprob <= CAST('2022-12-31' AS DATE FORMAT 'YYYY-MM-DD')
) WITH DATA NO PRIMARY INDEX;




/************************MONGO DB*****************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en MONGODB----------------------

CREATE MULTISET TABLE bddwestg.tmp093168_kpi14_detcpeval_mdb
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
FROM bddwestg.t12734cas514det_mongodb a
INNER JOIN bddwestg.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
AND a.fec_comprob >= CAST('2022-01-01' AS DATE FORMAT 'YYYY-MM-DD')
AND a.fec_comprob <= CAST('2022-12-31' AS DATE FORMAT 'YYYY-MM-DD')
) WITH DATA NO PRIMARY INDEX;

/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional


CREATE MULTISET TABLE bddwestg.tmp093168_kpigr14_val_cnorigen AS
(
 SELECT ind_presdj,count(num_doc) as cant_comp_origen
 FROM bddwestg.tmp093168_kpi14_detcpeval_tr
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


---------2. Conteo en FVirtual


CREATE MULTISET TABLE bddwestg.tmp093168_kpigr14_val_cndestino1 AS
(
 SELECT ind_presdj,count(num_comprob) as cant_comp_destino1
 FROM bddwestg.tmp093168_kpi14_detcpeval_fv
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


--------3 Conteo en MongoDB


CREATE MULTISET TABLE bddwestg.tmp093168_kpigr14_val_cndestino2 AS
(
 SELECT ind_presdj,count(num_comprob) as cant_comp_destino2
 FROM bddwestg.tmp093168_kpi14_detcpeval_mdb
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;




/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K014012022'  AND FEC_CARGA=CURRENT_DATE;

 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
 SELECT '2022',z.ind_presdj,
        'K014012022' ,
         CURRENT_DATE,
         SUM(z.cant_origen),
         SUM(z.cant_destino)
 FROM
  (
   SELECT
          x0.ind_presdj,
          x0.cant_comp_origen as cant_origen,
          coalesce(x1.cant_comp_destino1,0) as cant_destino
   FROM bddwestg.tmp093168_kpigr14_val_cnorigen x0
   LEFT JOIN bddwestg.tmp093168_kpigr14_val_cndestino1 x1 
   ON x0.ind_presdj=x1.ind_presdj
  ) z
 GROUP BY 1,2,3,4
 ;


    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K014022022'  AND FEC_CARGA=CURRENT_DATE;



    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
 SELECT  '2022',z.ind_presdj,
         'K014022022',
         CURRENT_DATE,
         SUM(z.cant_origen),
         SUM(z.cant_destino)
 FROM
  (
   SELECT x0.ind_presdj,
          x0.cant_comp_destino1 AS cant_origen,
          coalesce(x1.cant_comp_destino2,0) AS cant_destino
   FROM bddwestg.tmp093168_kpigr14_val_cndestino1 x0
   LEFT JOIN bddwestg.tmp093168_kpigr14_val_cndestino2 x1 
   ON x0.ind_presdj=x1.ind_presdj
  ) z
 GROUP BY 1,2,3,4
;


/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 



   CREATE MULTISET TABLE bddwestg.tmp093168_dif_K014012022 AS (
     SELECT DISTINCT 
   'K014012022' cod_kpi,
   ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_ruc_emisor,
   cod_tip_doc,
   ser_doc,
   num_doc
 

 FROM (
  SELECT   ann_ejercicio,
     num_ruc,
     ind_presdj,
     num_ruc_emisor,
     cod_tip_doc,
     ser_doc,
     num_doc
  FROM bddwestg.tmp093168_kpi14_detcpeval_tr
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob
  FROM bddwestg.tmp093168_kpi14_detcpeval_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;





 CREATE MULTISET TABLE bddwestg.tmp093168_dif_K014022022 AS (
 SELECT DISTINCT 
   'K014022022' cod_kpi,
   ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob
 
 FROM (
     SELECT  ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob
  FROM bddwestg.tmp093168_kpi14_detcpeval_fv
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob
  FROM bddwestg.tmp093168_kpi14_detcpeval_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;


/*************Archivo de diferencias *********************/
LOCK ROW FOR ACCESS
SELECT * FROM bddwestg.tmp093168_dif_K014012022 ;

LOCK ROW FOR ACCESS
SELECT * FROM bddwestg.tmp093168_dif_K014022022;
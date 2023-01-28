/*
DROP TABLE bddwestg.tmp093168_kpi15_detcpeval_tr;
DROP TABLE bddwestg.tmp093168_kpi15_detcpeval_fv;
DROP TABLE bddwestg.tmp093168_kpi15_detcpeval_mdb;
DROP TABLE bddwestg.tmp093168_kpigr15_cnorigen;
DROP TABLE bddwestg.tmp093168_kpigr15_cndestino1;
DROP TABLE bddwestg.tmp093168_kpigr15_cndestino2;
DROP TABLE bddwestg.tmp093168_dif_K015012022;
DROP TABLE bddwestg.tmp093168_dif_K015022022;
*/


/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

------------Genera Detalle Transaccional Comprobantes Válidos-------------------------------


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi15_detcpeval_tr
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
 TRIM(x0.num_doc) as num_doc,
 x0.mto_doc_fin_mn,
 x0.mto_deduccion_fin
FROM BDDWESTG.t8156cpgastodeduc x0 
LEFT JOIN BDDWESTG.ddp x1 ON x0.num_ruc_emisor = x1.ddp_numruc 
LEFT JOIN BDDWESTG.tmp093168_kpiperindj x2 on x0.num_ruc=x2.num_ruc
WHERE x0.ann_ejercicio = '2022' 
AND x0.ind_tip_gasto = '05' 
AND x0.ind_estado = '1'
AND x0.fec_doc >= CAST('2022-01-01' AS DATE FORMAT 'YYYY-MM-DD') 
AND x0.fec_doc <= CAST('2022-12-31' AS DATE FORMAT 'YYYY-MM-DD')
)
WITH DATA NO PRIMARY INDEX;



/***************************FVIRTUAL**************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en FVIRTUAL----------------------

CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi15_detcpeval_fv
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
FROM BDDWESTG.t12734cas514det a
INNER JOIN BDDWESTG.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
) WITH DATA NO PRIMARY INDEX;



/************************MONGO DB*****************************************************/

------- Genera Detalle MONGODB----------------------


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi15_detcpeval_mdb
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
FROM BDDWESTG.t12734cas514det_mongodb a
INNER JOIN BDDWESTG.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers <> '0'  -- valido
AND a.ind_archpers = '1'  -- personalizado
) WITH DATA NO PRIMARY INDEX;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr15_cnorigen AS
(
 SELECT ind_presdj,sum(mto_deduccion_fin) as mto_deduc_origen
 FROM BDDWESTG.tmp093168_kpi15_detcpeval_tr
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr15_cndestino1 AS
(
 SELECT ind_presdj,sum(mto_deduccion) as mto_deduc_destino1
 FROM BDDWESTG.tmp093168_kpi15_detcpeval_fv
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


--------3 Conteo en MongoDB



CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr15_cndestino2 AS
(
 SELECT ind_presdj,sum(mto_deduccion) as mto_deduc_destino2
 FROM BDDWESTG.tmp093168_kpi15_detcpeval_mdb
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K015012022'  AND FEC_CARGA=CURRENT_DATE;

    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO)
 SELECT '2022',z.ind_presdj,
        'K015012022' ,
         CURRENT_DATE,
         SUM(z.mto_origen),
         SUM(z.mto_destino)
 FROM
  (
   SELECT
          x0.ind_presdj,
          x0.mto_deduc_origen as mto_origen,
          coalesce(x1.mto_deduc_destino1,0) as mto_destino
   FROM BDDWESTG.tmp093168_kpigr15_cnorigen x0
   LEFT JOIN BDDWESTG.tmp093168_kpigr15_cndestino1 x1 
   ON x0.ind_presdj=x1.ind_presdj
  ) z
 GROUP BY 1,2,3,4
 ;

    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K015022022'  AND FEC_CARGA=CURRENT_DATE;

    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO)
 SELECT  '2022',z.ind_presdj,
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


/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 


    CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K015012022 AS (
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
  SELECT   ann_ejercicio,
     num_ruc,
     ind_presdj,
     num_ruc_emisor,
     cod_tip_doc,
     ser_doc,
     num_doc,
     mto_deduccion_fin
  FROM BDDWESTG.tmp093168_kpi15_detcpeval_tr
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob,
   mto_deduccion
  FROM BDDWESTG.tmp093168_kpi15_detcpeval_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;




 CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K015022022 AS (
 SELECT DISTINCT 
   'K015022022' cod_kpi,
   y0.ann_ejercicio,
   y0.num_ruc,
   y0.ind_presdj,
   y0.num_doc_emisor,
   y0.cod_tip_comprob,
   y0.num_serie,
   y0.num_comprob,
   y0.mto_deduccion
 
 FROM (
     SELECT  ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob,
   mto_deduccion
  FROM BDDWESTG.tmp093168_kpi15_detcpeval_fv
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   ind_presdj,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob,
   mto_deduccion
  FROM BDDWESTG.tmp093168_kpi15_detcpeval_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;


 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K015012022 ;
 
    LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K015022022;


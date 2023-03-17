/*
DROP TABLE BDDWESTG.tmp093168_kpi17_detcpeval_tr;
DROP TABLE BDDWESTG.tmp093168_kpi17_detcpeval_fv;
DROP TABLE BDDWESTG.tmp093168_kpi17_detcpeval_mdb;
DROP TABLE BDDWESTG.tmp093168_kpigr17_cnorigen;
DROP TABLE BDDWESTG.tmp093168_kpigr17_cndestino1;
DROP TABLE BDDWESTG.tmp093168_kpigr17_cndestino2;
*/

/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

------------Genera Detalle Transaccional Comprobantes Válidos-------------------------------


DROP TABLE BDDWESTG.tmp093168_kpi17_detcpeval_tr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi17_detcpeval_tr
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
AND x0.ind_tip_gasto = '03' 
AND x0.ind_estado = '1'
AND x0.fec_doc >= CAST('2022-01-01' AS DATE FORMAT 'YYYY-MM-DD') 
AND x0.fec_doc <= CAST('2022-12-31' AS DATE FORMAT 'YYYY-MM-DD')
AND (substr(x0.num_ruc,1,1) <>'2' OR  x0.num_ruc in (select num_ruc from BDDWESTG.tmp093168_rucs20_incluir))
)
WITH DATA NO PRIMARY INDEX;


/***************************FVIRTUAL**************************************************/

------- Genera Detalle en FVIRTUAL----------------------

DROP TABLE BDDWESTG.tmp093168_kpi17_detcpeval_fv;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi17_detcpeval_fv
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
WHERE a.cod_tip_gasto = '03'
AND a.ind_archpers = '1'  
AND a.ind_est_archpers <> '0'
) WITH DATA NO PRIMARY INDEX;


/************************MONGO DB*****************************************************/

------- Genera Detalle MONGODB----------------------


DROP TABLE BDDWESTG.tmp093168_kpi17_detcpeval_mdb;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi17_detcpeval_mdb
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
WHERE a.cod_tip_gasto = '03'
AND a.ind_archpers = '1'  
AND a.ind_est_archpers <> '0'
) WITH DATA NO PRIMARY INDEX;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional


DROP TABLE BDDWESTG.tmp093168_kpigr17_cnorigen;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr17_cnorigen AS
(
 SELECT ind_presdj,sum(mto_deduccion_fin) as mto_deduc_origen
 FROM BDDWESTG.tmp093168_kpi17_detcpeval_tr
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual

DROP TABLE BDDWESTG.tmp093168_kpigr17_cndestino1;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr17_cndestino1 AS
(
 SELECT ind_presdj,sum(mto_deduccion) as mto_deduc_destino1
 FROM BDDWESTG.tmp093168_kpi17_detcpeval_fv
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB

DROP TABLE BDDWESTG.tmp093168_kpigr17_cndestino2;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr17_cndestino2 AS
(
 SELECT ind_presdj,sum(mto_deduccion) as mto_deduc_destino2
 FROM BDDWESTG.tmp093168_kpi17_detcpeval_mdb
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 

DROP TABLE BDDWESTG.tmp093168_dif_K017012022 ;
    CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K017012022 AS (
     SELECT DISTINCT 
   y0.ann_ejercicio,
   y0.num_ruc as num_ruc_trab,
   y0.num_ruc_emisor,
   y0.cod_tip_doc,
   y0.ser_doc,
   y0.num_doc,
   y0.mto_deduccion_fin as mto_deduccion

 FROM (
  SELECT   ann_ejercicio,
     num_ruc,
     num_ruc_emisor,
     cod_tip_doc,
     ser_doc,
     num_doc,
     mto_deduccion_fin
  FROM BDDWESTG.tmp093168_kpi17_detcpeval_tr
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob,
   mto_deduccion
  FROM BDDWESTG.tmp093168_kpi17_detcpeval_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;

 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K017012022
 ORDER BY num_ruc_trab,num_ruc_emisor;


DROP TABLE BDDWESTG.tmp093168_dif_K017022022 ;
CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K017022022 AS (
 SELECT DISTINCT 
   y0.ann_ejercicio,
   y0.num_ruc as num_ruc_trab,
   y0.num_doc_emisor as num_ruc_emisor,
   y0.cod_tip_comprob,
   y0.num_serie,
   y0.num_comprob,
   y0.mto_deduccion
 
 FROM (
     SELECT  ann_ejercicio,
   num_ruc,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob,
   mto_deduccion
  FROM BDDWESTG.tmp093168_kpi17_detcpeval_fv
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob,
   mto_deduccion
  FROM BDDWESTG.tmp093168_kpi17_detcpeval_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;


    LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K017022022
 ORDER BY num_ruc_trab,num_ruc_emisor;

/*****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K017012022'  AND FEC_CARGA=CURRENT_DATE;

   
 INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
 SELECT
  '2022',       
  x0.ind_presdj,
  'K017012022' ,
  CURRENT_DATE,
  case when x0.ind_presdj=0 then (select coalesce(sum(mto_deduc_origen),0) from BDDWESTG.tmp093168_kpigr17_cnorigen) else 0 end as mto_origen,
  coalesce(x1.mto_deduc_destino1,0) as mto_destino,
  case when x0.ind_presdj=0 then 
  case when (select  coalesce(sum(mto_deduccion),0) from BDDWESTG.tmp093168_dif_K017012022)=0 then 1 else 0 end 
  end as ind_incuniv,
  case when x0.ind_presdj=0 then (select coalesce(sum(mto_deduccion),0) from BDDWESTG.tmp093168_dif_K017012022) END as cnt_regdif
 FROM 
 (
   select y.ind_presdj,SUM(y.mto_deduc_origen) as mto_deduc_origen
   from
   (
    select * from BDDWESTG.tmp093168_kpigr17_cnorigen
    union all select 1,0 from (select '1' agr1) a
    union all select 0,0 from (select '0' agr0) b
   ) y group by 1
 ) x0
 LEFT JOIN BDDWESTG.tmp093168_kpigr17_cndestino1 x1 
 ON x0.ind_presdj=x1.ind_presdj
 ;


    DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K017022022'  AND FEC_CARGA=CURRENT_DATE;

 INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
 SELECT 
   '2022',
   x0.ind_presdj,
            'K017022022',
   CURRENT_DATE,   
   x0.mto_deduc_destino1 AS mto_origen,
   case when x0.ind_presdj=0  then (select coalesce(sum(mto_deduc_destino2),0) from BDDWESTG.tmp093168_kpigr17_cndestino2) else 0 end AS mto_destino,
   case when x0.ind_presdj=0 then 
    case when (select  coalesce(sum(mto_deduccion),0) from BDDWESTG.tmp093168_dif_K017022022)=0 then 1 else 0 end 
   end as ind_incuniv,
   case when x0.ind_presdj=0 then (select COALESCE(sum(mto_deduccion),0) from BDDWESTG.tmp093168_dif_K017022022) END as cnt_regdif
 FROM 
 (
  select y.ind_presdj,SUM(y.mto_deduc_destino1) as mto_deduc_destino1
   from
   (
    select * from BDDWESTG.tmp093168_kpigr17_cndestino1
    union all select 1,0 from (select '1' agr1) a
    union all select 0,0 from (select '0' agr0) b
   ) y group by 1
 ) x0
 LEFT JOIN BDDWESTG.tmp093168_kpigr17_cndestino2 x1 
 ON x0.ind_presdj=x1.ind_presdj
 ;



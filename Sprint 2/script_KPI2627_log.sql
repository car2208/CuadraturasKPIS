/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

------------Genera Detalle Transaccional Comprobantes VÃ¡lidos-------------------------------

--DROP TABLE bddwestg.tmp093168_kpi26_detcpeval_tr;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi26_detcpeval_tr
AS
(
SELECT
 DISTINCT 
 coalesce(x2.ind_presdj,0) as ind_presdj,
 TRIM(x0.num_ruc) as num_ruc,
 x0.per_pag as per_pago,
 x0.num_formul,
 x0.num_doc as num_ordope,
 x0.MTO_BASIMP as mto_gravado
FROM bddwestg.t7910pagorta x0 
LEFT JOIN bddwestg.ddp x1 ON x0.num_ruc = x1.ddp_numruc
LEFT JOIN bddwestg.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
WHERE x0.per_pag between '202201' and '202212'
AND x0.ind_tippag = '3'

)
WITH DATA NO PRIMARY INDEX;



--DROP TABLE bddwestg.tmp093168_kpi26_detcpeval_fv;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi26_detcpeval_fv
AS
(
SELECT 
        DISTINCT 
  TRIM(b.num_ruc) as num_ruc,
  b.ind_presdj,
  SUBSTR(a.per_pago,3,4)||SUBSTR(a.per_pago,1,2) as per_pago,
  cast(a.num_formul as smallint) num_formul,
  cast(a.NUM_ORDOPE as integer) num_ordope,
  a.MTO_GRAVADO 
FROM bddwestg.T7993CAS100DET a
INNER JOIN bddwestg.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
) WITH DATA NO PRIMARY INDEX;



--DROP TABLE bddwestg.tmp093168_kpi26_detcpeval_mdb;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi26_detcpeval_mdb
AS
(
SELECT
  DISTINCT 
  TRIM(b.num_ruc) as num_ruc,
  b.ind_presdj,
  SUBSTR(a.per_pago,3,4)||SUBSTR(a.per_pago,1,2) as per_pago,
  cast(a.num_formul as smallint) num_formul,
  cast(a.NUM_ORDOPE as integer) num_ordope,
  a.MTO_GRAVADO 
FROM bddwestg.T7993CAS100DET_MONGODB a
INNER JOIN bddwestg.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
) WITH DATA NO PRIMARY INDEX;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/


--DROP TABLE bddwestg.tmp093168_kpigr26_val_cnorigen;


CREATE MULTISET TABLE bddwestg.tmp093168_kpigr26_val_cnorigen AS
(
 SELECT ind_presdj,count(num_ordope) as cant_comp_origen, sum(mto_gravado) as mto_origen
 FROM bddwestg.tmp093168_kpi26_detcpeval_tr
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;



--DROP TABLE bddwestg.tmp093168_kpigr26_val_cndestino1;


CREATE MULTISET TABLE bddwestg.tmp093168_kpigr26_val_cndestino1 AS
(
 SELECT ind_presdj,count(NUM_ORDOPE) as cant_comp_destino1, sum(MTO_GRAVADO) as mto_destino1
 FROM bddwestg.tmp093168_kpi26_detcpeval_fv
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;



--DROP TABLE bddwestg.tmp093168_kpigr26_val_cndestino2 ;


CREATE MULTISET TABLE bddwestg.tmp093168_kpigr26_val_cndestino2 AS
(
 SELECT ind_presdj,count(NUM_ORDOPE) as cant_comp_destino2, sum(MTO_GRAVADO) as mto_destino2
 FROM bddwestg.tmp093168_kpi26_detcpeval_mdb
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;



/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K027012022'  AND FEC_CARGA=CURRENT_DATE;

    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
 SELECT '2022',z.ind_presdj,
        'K027012022' ,
         CURRENT_DATE,
         SUM(z.cant_origen),
         SUM(z.cant_destino)
 FROM
  (
   SELECT
          x0.ind_presdj,
       case when x0.ind_presdj=0 then (select sum(cant_comp_origen) from bddwestg.tmp093168_kpigr26_val_cnorigen) else 0 end as cant_origen,
          coalesce(x1.cant_comp_destino1,0) as cant_destino
   FROM bddwestg.tmp093168_kpigr26_val_cnorigen x0
   LEFT JOIN bddwestg.tmp093168_kpigr26_val_cndestino1 x1 
   ON x0.ind_presdj=x1.ind_presdj
   WHERE cant_origen<>0 and cant_destino<>0
  ) z
 GROUP BY 1,2,3,4
 ;



    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K027022022'  AND FEC_CARGA=CURRENT_DATE;

    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
 SELECT  '2022',z.ind_presdj,
         'K027022022',
         CURRENT_DATE,
         SUM(z.cant_origen),
         SUM(z.cant_destino)
 FROM
  (
   SELECT x0.ind_presdj,
          x0.cant_comp_destino1 AS cant_origen,
       case when x0.ind_presdj=0  then (select sum(cant_comp_destino2) from bddwestg.tmp093168_kpigr26_val_cndestino2) else 0 end AS cant_destino
   FROM bddwestg.tmp093168_kpigr26_val_cndestino1 x0
   LEFT JOIN bddwestg.tmp093168_kpigr26_val_cndestino2 x1 
   ON x0.ind_presdj=x1.ind_presdj
   WHERE cant_origen<>0 and cant_destino<>0
  ) z
 GROUP BY 1,2,3,4
 ;


/***********************************************27******************************************************************************************************/ 

 DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K026012022'  AND FEC_CARGA=CURRENT_DATE;


    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO)
 SELECT '2022',z.ind_presdj,
        'K026012022' ,
         CURRENT_DATE,
         SUM(z.mto_origen),
         SUM(z.mto_destino)
 FROM
  (
   SELECT
          x0.ind_presdj,
       case when x0.ind_presdj=0 then (select sum(mto_origen) from bddwestg.tmp093168_kpigr26_val_cnorigen) else 0 end as mto_origen,
          coalesce(x1.cant_comp_destino1,0) as mto_destino
   FROM bddwestg.tmp093168_kpigr26_val_cnorigen x0
   LEFT JOIN bddwestg.tmp093168_kpigr26_val_cndestino1 x1 
   ON x0.ind_presdj=x1.ind_presdj
    WHERE mto_origen<>0 and mto_destino<>0
  ) z
 GROUP BY 1,2,3,4
 ;


    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K026022022'  AND FEC_CARGA=CURRENT_DATE;

    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO)
 SELECT  '2022',z.ind_presdj,
         'K026022022',
         CURRENT_DATE,
         SUM(z.mto_origen),
         SUM(z.mto_destino)
 FROM
  (
   SELECT x0.ind_presdj,
          x0.mto_destino1 AS mto_origen,
       case when x0.ind_presdj=0  then (select sum(mto_destino2) from bddwestg.tmp093168_kpigr26_val_cndestino2) else 0 end AS mto_destino
   FROM bddwestg.tmp093168_kpigr26_val_cndestino1 x0
   LEFT JOIN bddwestg.tmp093168_kpigr26_val_cndestino2 x1 
   ON x0.ind_presdj=x1.ind_presdj
   WHERE mto_origen<>0 and mto_destino<>0
  ) z
 GROUP BY 1,2,3,4
 ;



/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 


--DROP TABLE bddwestg.tmp093168_dif_K027012022 ;

    CREATE MULTISET TABLE bddwestg.tmp093168_dif_K027012022 AS (
     SELECT DISTINCT 
     y0.num_ruc,
     y0.per_pago,
     y0.num_formul,
     y0.num_ordope
 FROM (
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope
  FROM bddwestg.tmp093168_kpi26_detcpeval_tr
  EXCEPT ALL
  SELECT  num_ruc,
     per_pago,
     num_formul,
     num_ordope
  FROM bddwestg.tmp093168_kpi26_detcpeval_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;


 --.EXPORT FILE /work1/teradata/dat/093168/DIF_K027012022_CAS100_TRANVSFVIR_20230208.unl;

 LOCK ROW FOR ACCESS
 SELECT * FROM bddwestg.tmp093168_dif_K027012022 
 ORDER BY num_ruc,per_pago;


--DROP TABLE bddwestg.tmp093168_dif_K027022022 ;

 CREATE MULTISET TABLE bddwestg.tmp093168_dif_K027022022 AS (
 SELECT DISTINCT 
   y0.num_ruc,
     y0.per_pago,
     y0.num_formul,
     y0.num_ordope
 FROM (
     SELECT  num_ruc,
     per_pago,
     num_formul,
     num_ordope
  FROM bddwestg.tmp093168_kpi26_detcpeval_fv
  EXCEPT ALL
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope
  FROM bddwestg.tmp093168_kpi26_detcpeval_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;

 
 --.EXPORT FILE /work1/teradata/dat/093168/DIF_K027022022_CAS100_FVIRVSMODB_20230208.unl;


    LOCK ROW FOR ACCESS
 SELECT * FROM bddwestg.tmp093168_dif_K027022022
 ORDER BY num_ruc,per_pago;


/********************************************************************************/
--------------------------------------PARA EL 26----------------------------------

     --DROP TABLE bddwestg.tmp093168_dif_K026012022 ;
    CREATE MULTISET TABLE bddwestg.tmp093168_dif_K026012022 AS (
     SELECT DISTINCT 
     y0.num_ruc,
     y0.per_pago,
     y0.num_formul,
     y0.num_ordope,
     y0.mto_gravado
 FROM (
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope,
     mto_gravado
  FROM bddwestg.tmp093168_kpi26_detcpeval_tr
  EXCEPT ALL
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope,
     mto_gravado
  FROM bddwestg.tmp093168_kpi26_detcpeval_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;



-- .EXPORT FILE /work1/teradata/dat/093168/DIF_K027012022_CAS100_TRANVSFVIR_20230208.unl;


 LOCK ROW FOR ACCESS
 SELECT * FROM bddwestg.tmp093168_dif_K026012022 
 ORDER BY num_ruc,per_pago;

 --DROP TABLE bddwestg.tmp093168_dif_K026022022 ;
 CREATE MULTISET TABLE bddwestg.tmp093168_dif_K026022022 AS (
 SELECT DISTINCT 
   y0.num_ruc,
     y0.per_pago,
     y0.num_formul,
     y0.num_ordope,
     y0.mto_gravado
 FROM (
     SELECT  num_ruc,
     per_pago,
     num_formul,
     num_ordope,
     mto_gravado
  FROM bddwestg.tmp093168_kpi26_detcpeval_fv
  EXCEPT ALL
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope,
     mto_gravado
  FROM bddwestg.tmp093168_kpi26_detcpeval_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;

-- .EXPORT FILE /work1/teradata/dat/093168/DIF_K027022022_CAS100_FVIRVSMODB_20230208.unl;


    LOCK ROW FOR ACCESS
 SELECT * FROM bddwestg.tmp093168_dif_K026022022
 ORDER BY num_ruc,per_pago;



DROP TABLE bddwestg.tmp093168_kpi26_detcpeval_tr;
DROP TABLE bddwestg.tmp093168_kpi26_detcpeval_fv;
DROP TABLE bddwestg.tmp093168_kpi26_detcpeval_mdb;
DROP TABLE bddwestg.tmp093168_kpigr26_val_cnorigen;
DROP TABLE bddwestg.tmp093168_kpigr26_val_cndestino1;
DROP TABLE bddwestg.tmp093168_kpigr26_val_cndestino2 ;
-
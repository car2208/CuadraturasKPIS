/*
DROP TABLE BDDWESTG.tmp093168_kpi26_detcpeval_tr;
DROP TABLE BDDWESTG.tmp093168_kpi26_detcpeval_fv;
DROP TABLE BDDWESTG.tmp093168_kpi26_detcpeval_mdb;
DROP TABLE BDDWESTG.tmp093168_kpigr26_val_cnorigen;
DROP TABLE BDDWESTG.tmp093168_kpigr26_val_cndestino1;
DROP TABLE BDDWESTG.tmp093168_kpigr26_val_cndestino2;
*/



/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

------------Genera Detalle Transaccional Comprobantes Válidos-------------------------------



DROP TABLE BDDWESTG.tmp093168_kpi26_detcpeval_tr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi26_detcpeval_tr
AS
(
SELECT
 DISTINCT 
 coalesce(x2.ind_presdj,0) as ind_presdj,
 TRIM(x0.num_ruc) as num_ruc,
 x0.per_pag as per_pago,
 x0.num_formul,
 cast(x0.num_doc as bigint) as num_ordope,
 x0.MTO_BASIMP as mto_gravado
FROM BDDWESTG.t7910pagorta x0 
LEFT JOIN BDDWESTG.ddp x1 ON x0.num_ruc = x1.ddp_numruc
LEFT JOIN BDDWESTG.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
WHERE x0.per_pag between '202201' and '202212'
AND x0.ind_tippag = '3'
AND (substr(x0.num_ruc,1,1) <>'2' OR  x0.num_ruc in (select num_ruc from BDDWESTG.tmp093168_rucs20_incluir))

)
WITH DATA NO PRIMARY INDEX;


/***************************FVIRTUAL**************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en FVIRTUAL----------------------


DROP TABLE BDDWESTG.tmp093168_kpi26_detcpeval_fv;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi26_detcpeval_fv
AS
(
SELECT 
        DISTINCT 
  TRIM(b.num_ruc) as num_ruc,
  b.ind_presdj,
  SUBSTR(a.per_pago,3,4)||SUBSTR(a.per_pago,1,2) as per_pago,
  cast(a.num_formul as smallint) num_formul,
  cast(a.NUM_ORDOPE as bigint) num_ordope,
  a.MTO_GRAVADO 
FROM BDDWESTG.T7993CAS100DET a
INNER JOIN BDDWESTG.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
) WITH DATA NO PRIMARY INDEX;


/************************MONGO DB*****************************************************/

------- Genera Detalle de  COMPROBANTES VÁLIDOS en MONGODB----------------------

DROP TABLE BDDWESTG.tmp093168_kpi26_detcpeval_mdb;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi26_detcpeval_mdb
AS
(
SELECT
  DISTINCT 
  TRIM(b.num_ruc) as num_ruc,
  b.ind_presdj,
  SUBSTR(a.per_pago,3,4)||SUBSTR(a.per_pago,1,2) as per_pago,
  cast(a.num_formul as smallint) num_formul,
  cast(a.NUM_ORDOPE as bigint) num_ordope,
  a.MTO_GRAVADO 
FROM BDDWESTG.T7993CAS100DET_MONGODB a
INNER JOIN BDDWESTG.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
) WITH DATA NO PRIMARY INDEX;


/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional


DROP TABLE BDDWESTG.tmp093168_kpigr26_val_cnorigen;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr26_val_cnorigen AS
(
 SELECT ind_presdj,count(num_ordope) as cant_comp_origen, sum(mto_gravado) as mto_origen
 FROM BDDWESTG.tmp093168_kpi26_detcpeval_tr
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual

DROP TABLE BDDWESTG.tmp093168_kpigr26_val_cndestino1;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr26_val_cndestino1 AS
(
 SELECT ind_presdj,count(NUM_ORDOPE) as cant_comp_destino1, sum(MTO_GRAVADO) as mto_destino1
 FROM BDDWESTG.tmp093168_kpi26_detcpeval_fv
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB

DROP TABLE BDDWESTG.tmp093168_kpigr26_val_cndestino2 ;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr26_val_cndestino2 AS
(
 SELECT ind_presdj,count(NUM_ORDOPE) as cant_comp_destino2, sum(MTO_GRAVADO) as mto_destino2
 FROM BDDWESTG.tmp093168_kpi26_detcpeval_mdb
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 

DROP TABLE BDDWESTG.tmp093168_dif_K027012022 ;
   CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K027012022 AS (
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
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_tr
  EXCEPT ALL
  SELECT  num_ruc,
     per_pago,
     num_formul,
     num_ordope
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;


 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K027012022 
 ORDER BY num_ruc,per_pago;




DROP TABLE BDDWESTG.tmp093168_dif_K027022022 ;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K027022022 AS (
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
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_fv
  EXCEPT ALL
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;

 
    LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K027022022
 ORDER BY num_ruc,per_pago;



/********************INSERT EN TABLA FINAL KPI27***********************************/
    
    DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K027012022'  AND FEC_CARGA=CURRENT_DATE;

 INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
 SELECT
   '2022',
   x0.ind_presdj,
   'K027012022' ,
   CURRENT_DATE,
   case when x0.ind_presdj=0 then 
   (select coalesce(sum(cant_comp_origen),0) from BDDWESTG.tmp093168_kpigr26_val_cnorigen) 
   else 0 end as cant_origen,
   coalesce(x1.cant_comp_destino1,0) as cant_destino,
   case when x0.ind_presdj=0 then 
   case when (select count(*) from BDDWESTG.tmp093168_dif_K027012022)=0 then 1 else 0 end 
   end as ind_incuniv,
   case when x0.ind_presdj=0 then (select count(*) from BDDWESTG.tmp093168_dif_K027012022) END as cnt_regdif
 FROM
 (
  select y.ind_presdj,SUM(y.cant_comp_origen) as cant_comp_origen
  from
   (
    select * from BDDWESTG.tmp093168_kpigr26_val_cnorigen
    union all select 1,0,0 from (select '1' agr1) a
    union all select 0,0,0 from (select '0' agr0) b
   ) y group by 1
 ) x0
 LEFT JOIN BDDWESTG.tmp093168_kpigr26_val_cndestino1 x1 
 ON x0.ind_presdj=x1.ind_presdj
 ;


   DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K027022022'  AND FEC_CARGA=CURRENT_DATE;


 INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
 SELECT 
   '2022',
   x0.ind_presdj,
   'K027022022',
   CURRENT_DATE,
   x0.cant_comp_destino1 AS cant_origen,
   case when x0.ind_presdj=0  then (select coalesce(sum(cant_comp_destino2),0) from BDDWESTG.tmp093168_kpigr26_val_cndestino2) else 0 end AS cant_destino,
   case when x0.ind_presdj=0 then 
   case when (select count(*) from BDDWESTG.tmp093168_dif_K027022022)=0 then 1 else 0 end 
   end as ind_incuniv,
   case when x0.ind_presdj=0 then (select count(*) from BDDWESTG.tmp093168_dif_K027022022) END as cnt_regdif
 FROM
 (
  select y.ind_presdj,SUM(y.cant_comp_destino1) as cant_comp_destino1
  from
   (
    select * from BDDWESTG.tmp093168_kpigr26_val_cndestino1
    union all select 1,0,0 from (select '1' agr1) a
    union all select 0,0,0 from (select '0' agr0) b
   ) y group by 1
 ) x0
 LEFT JOIN BDDWESTG.tmp093168_kpigr26_val_cndestino2 x1 
 ON x0.ind_presdj=x1.ind_presdj
 ;



/********************************************************************************/
--------------------------------------PARA EL 26----------------------------------



DROP TABLE BDDWESTG.tmp093168_dif_K026012022 ;
   CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K026012022 AS (
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
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_tr
  EXCEPT ALL
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope,
     mto_gravado
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;


 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K026012022 
 ORDER BY num_ruc,per_pago;



DROP TABLE BDDWESTG.tmp093168_dif_K026022022 ;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K026022022 AS (
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
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_fv
  EXCEPT ALL
  SELECT   num_ruc,
     per_pago,
     num_formul,
     num_ordope,
     mto_gravado
  FROM BDDWESTG.tmp093168_kpi26_detcpeval_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;


    LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K026022022
 ORDER BY num_ruc,per_pago;


/***********************************************26******************************************************************************************************/ 

 DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K026012022'  AND FEC_CARGA=CURRENT_DATE;


 INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
 SELECT
   '2022',
   x0.ind_presdj,
   'K026012022' ,
   CURRENT_DATE,
   case when x0.ind_presdj=0 then (select coalesce(sum(mto_origen),0) from BDDWESTG.tmp093168_kpigr26_val_cnorigen) else 0 end as mto_origen,
   coalesce(x1.mto_destino1,0) as mto_destino,
    case when x0.ind_presdj=0 then 
   case when (select  coalesce(sum(mto_gravado),0) from BDDWESTG.tmp093168_dif_K026012022)=0 then 1 else 0 end 
   end as ind_incuniv,
   case when x0.ind_presdj=0 then (select coalesce(sum(mto_gravado),0) from BDDWESTG.tmp093168_dif_K026012022) END as cnt_regdif
 FROM
 (
  select y.ind_presdj,SUM(y.mto_origen) as mto_origen
   from
   (
    select * from BDDWESTG.tmp093168_kpigr26_val_cnorigen
    union all select 1,0,0 from (select '1' agr1) a
    union all select 0,0,0 from (select '0' agr0) b
   ) y group by 1

 ) x0
 LEFT JOIN BDDWESTG.tmp093168_kpigr26_val_cndestino1 x1 
 ON x0.ind_presdj=x1.ind_presdj
 ;


    DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K026022022'  AND FEC_CARGA=CURRENT_DATE;


    
 INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,MTO_REGORIGEN,MTO_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
 SELECT 
   '2022',
   x0.ind_presdj,
   'K026022022',
   CURRENT_DATE,
   x0.mto_destino1 AS mto_origen,
   case when x0.ind_presdj=0  then (select coalesce(sum(mto_destino2),0) from BDDWESTG.tmp093168_kpigr26_val_cndestino2) else 0 end AS mto_destino,
   case when x0.ind_presdj=0 then 
   case when (select  coalesce(sum(mto_gravado),0) from BDDWESTG.tmp093168_dif_K026022022)=0 then 1 else 0 end 
   end as ind_incuniv,
   case when x0.ind_presdj=0 then (select coalesce(sum(mto_gravado),0) from BDDWESTG.tmp093168_dif_K026022022) END as cnt_regdif   
 FROM 
 (
  select y.ind_presdj,SUM(y.mto_destino1) as mto_destino1
   from
   (
    select * from BDDWESTG.tmp093168_kpigr26_val_cndestino1
    union all select 1,0,0 from (select '1' agr1) a
    union all select 0,0,0 from (select '0' agr0) b
   ) y group by 1
 ) x0
 LEFT JOIN BDDWESTG.tmp093168_kpigr26_val_cndestino2 x1 
 ON x0.ind_presdj=x1.ind_presdj
 ;


/*********************************************************************************/



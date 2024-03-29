/*
DROP TABLE bddwestg.tmp093168_kpi14_detcpeobs_tr;
DROP TABLE bddwestg.tmp093168_kpi14_detcpeobs_fv;
DROP TABLE bddwestg.tmp093168_kpi14_detcpeobs_mdb;
DROP TABLE bddwestg.tmp093168_kpigr14_obs_cnorigen;
DROP TABLE bddwestg.tmp093168_kpigr14_obs_cndestino1;
DROP TABLE bddwestg.tmp093168_kpigr14_obs_cndestino2;
DROP TABLE bddwestg.tmp093168_dif_K014032022;
DROP TABLE bddwestg.tmp093168_dif_K014042022;

*/


/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

------------Genera Detalle Transaccional-------------------------------

--DROP TABLE bddwestg.tmp093168_kpi14_detcpeobs_tr;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi14_detcpeobs_tr as
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
 FROM bddwestg.t8157cpgastoobserv x0
 LEFT JOIN bddwestg.ddp x1 ON x0.num_ruc_emisor = x1.ddp_numruc
 LEFT JOIN bddwestg.tmp093168_kpiperindj x2 ON x0.num_ruc=x2.num_ruc
 WHERE x0.ann_ejercicio = '2022' 
 AND x0.ind_tip_gasto = '05' 
 AND x0.fec_pago >= CAST('2022-01-01' AS DATE FORMAT 'YYYY-MM-DD') 
 AND x0.fec_pago <= CAST('2022-12-31' AS DATE FORMAT 'YYYY-MM-DD')
 AND x0.ind_inconsistencia <> 'I1'
 AND (substr(x0.num_ruc,1,1) <>'2' OR  x0.num_ruc in (select num_ruc from bddwestg.tmp093168_rucs20_incluir))
) WITH DATA PRIMARY INDEX (num_ruc,num_ruc_emisor,cod_tip_doc,ser_doc,num_doc);


/***************************FVIRTUAL**************************************************/

------ Genera Detalle de COMPROBANTES OBSERVADOS en FVIRTUAL-------------------


--DROP TABLE bddwestg.tmp093168_kpi14_detcpeobs_fv;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi14_detcpeobs_fv
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
 AND a.ind_est_archpers = '0'  -- OBSERVADO
 AND a.ind_archpers = '1'
) WITH DATA PRIMARY INDEX (ann_ejercicio,num_ruc,num_doc_emisor,cod_tip_comprob,num_serie,num_comprob);


/************************MONGO DB*****************************************************/

------ Genera Detalle de COMPROBANTES OBSERVADOS en MONGODB-------------------

--DROP TABLE bddwestg.tmp093168_kpi14_detcpeobs_mdb;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi14_detcpeobs_mdb
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
FROM bddwestg.t12734cas514det_mongodb a
INNER JOIN bddwestg.tmp093168_kpiperindj b ON a.num_sec = b.num_sec 
WHERE a.cod_tip_gasto = '05'
AND a.ind_est_archpers = '0'  -- OBSERVADO
AND a.ind_archpers = '1'  -- personalizado
) WITH DATA PRIMARY INDEX (ann_ejercicio,num_ruc,num_doc_emisor,cod_tip_comprob,num_serie,num_comprob);

/*************************************************************************************/
/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

--DROP TABLE bddwestg.tmp093168_kpigr14_obs_cnorigen;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr14_obs_cnorigen AS
(
 SELECT ind_presdj,count(num_doc) as cant_comp_origen
 FROM bddwestg.tmp093168_kpi14_detcpeobs_tr
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


---------2. Conteo en FVirtual


--DROP TABLE bddwestg.tmp093168_kpigr14_obs_cndestino1;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr14_obs_cndestino1 AS
(
 SELECT ind_presdj,count(num_comprob) as cant_comp_destino1
 FROM bddwestg.tmp093168_kpi14_detcpeobs_fv
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB

--DROP TABLE bddwestg.tmp093168_kpigr14_obs_cndestino2;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr14_obs_cndestino2 AS
(
 SELECT ind_presdj,count(num_comprob) as cant_comp_destino2
 FROM bddwestg.tmp093168_kpi14_detcpeobs_mdb
 GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 

--DROP TABLE bddwestg.tmp093168_dif_K014032022 ;
    CREATE MULTISET TABLE bddwestg.tmp093168_dif_K014032022 AS (
     SELECT DISTINCT 
   y0.ann_ejercicio,
   y0.num_ruc as num_ruc_trab,
   y0.num_ruc_emisor,
   y0.cod_tip_doc,
   y0.ser_doc,
   y0.num_doc
 

 FROM (
  SELECT   ann_ejercicio,
     num_ruc,
     num_ruc_emisor,
     cod_tip_doc,
     ser_doc,
     num_doc
  FROM bddwestg.tmp093168_kpi14_detcpeobs_tr
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob
  FROM bddwestg.tmp093168_kpi14_detcpeobs_fv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;

--DROP TABLE bddwestg.tmp093168_dif_K014042022 ;
 CREATE MULTISET TABLE bddwestg.tmp093168_dif_K014042022 AS (
 SELECT DISTINCT 
   y0.ann_ejercicio,
   y0.num_ruc as num_ruc_trab,
   y0.num_doc_emisor as num_ruc_emisor,
   y0.cod_tip_comprob,
   y0.num_serie,
   y0.num_comprob
 
 FROM (
     SELECT  ann_ejercicio,
   num_ruc,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob
  FROM bddwestg.tmp093168_kpi14_detcpeobs_fv
  EXCEPT ALL
  SELECT  ann_ejercicio,
   num_ruc,
   num_doc_emisor,
   cod_tip_comprob,
   num_serie,
   num_comprob
  FROM bddwestg.tmp093168_kpi14_detcpeobs_mdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;

/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K014032022'  AND FEC_CARGA=CURRENT_DATE;

 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF)
  SELECT 
         '2022',
         x0.ind_presdj,
         'K014032022',
         CURRENT_DATE,
         case when x0.ind_presdj=0 then (select sum(cant_comp_origen) from bddwestg.tmp093168_kpigr14_obs_cnorigen) else 0 end as cant_origen,
         coalesce(x1.cant_comp_destino1,0) as cant_destino,
         case when x0.ind_presdj=0 then 
            case when (select count(*) from bddwestg.tmp093168_dif_K014032022)=0 then 1 else 0 end 
         end as ind_incuniv,
         case when x0.ind_presdj=0 then (select count(*) from bddwestg.tmp093168_dif_K014032022) END as cnt_regdif
   FROM (
				select y.ind_presdj,SUM(y.cant_comp_origen) as cant_comp_origen
				from
					(
						select * from bddwestg.tmp093168_kpigr14_obs_cnorigen
						union all select 1,0 from (select '1' agr1) a
						union all select 0,0 from (select '0' agr0) b
					) y group by 1
			) x0
   LEFT JOIN bddwestg.tmp093168_kpigr14_obs_cndestino1 x1 
   ON  x0.ind_presdj=x1.ind_presdj
 ;


    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K014042022'  AND FEC_CARGA=CURRENT_DATE;

    
 INSERT INTO bddwestg.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,,IND_INCUNIV,CNT_REGDIF)
 SELECT 
         '2022',
         x0.ind_presdj,
         'K014042022',
         CURRENT_DATE,
         x0.cant_comp_destino1 AS cant_origen,
         case when x0.ind_presdj=0  then (select sum(cant_comp_destino2) from bddwestg.tmp093168_kpigr14_obs_cndestino2) else 0 end AS cant_destino,
         case when x0.ind_presdj=0 then 
            case when (select count(*) from bddwestg.tmp093168_dif_K014042022)=0 then 1 else 0 end 
          end as ind_incuniv,
         case when x0.ind_presdj=0 then (select count(*) from bddwestg.tmp093168_dif_K014042022) END as cnt_regdif
  FROM (
					select y.ind_presdj,SUM(y.cant_comp_destino1) as cant_comp_destino1
					from
					(
						select * from bddwestg.tmp093168_kpigr14_obs_cndestino1
						union all select 1,0 from (select '1' agr1) a
						union all select 0,0 from (select '0' agr0) b
					) y group by 1

			) x0
   LEFT JOIN bddwestg.tmp093168_kpigr14_obs_cndestino2 x1 
   ON x0.ind_presdj=x1.ind_presdj
 ;

/***************Archivo de diferencias *******************************/

 LOCK ROW FOR ACCESS
 SELECT * FROM bddwestg.tmp093168_dif_K014032022
 ORDER BY num_ruc_trab,num_ruc_emisor;

    LOCK ROW FOR ACCESS
 SELECT * FROM bddwestg.tmp093168_dif_K014042022
 ORDER BY num_ruc_trab,num_ruc_emisor;


/*
  SELECT   x0.ann_ejercicio,
     x0.num_ruc,
     x0.num_ruc_emisor,
     x0.cod_tip_doc,
     x0.ser_doc,
     x0.num_doc
  FROM bddwestgd.tmp093168_kpi14_detcpeobs_tr x0
  LEFT JOIN bddwestgd.tmp093168_kpi14_detcpeobs_fv x1
  ON 
  x0.ann_ejercicio=x1.ann_ejercicio and 
  x0.num_ruc=x1.num_ruc and 
  x0.num_ruc_emisor=x1.num_doc_emisor and
  x0.cod_tip_doc=x1.cod_tip_comprob and 
  x0.ser_doc=x1.num_serie and 
  x0.num_doc=x1.num_comprob
  where x1.num_ruc is null
  */
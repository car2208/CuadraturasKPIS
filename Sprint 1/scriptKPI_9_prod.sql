
/*========================================================================================= */
/**********************************TRANSACCIONALES******************************************/
/*========================================================================================= */

/*******************Obtiene última dj form 0601********************************************/


/**********Obtiene Última DJ Form 0601 ********************************/
DROP TABLE bddwestg.tmp093168_udjkpigr9;
CREATE MULTISET TABLE bddwestg.tmp093168_udjkpigr9 as
(
  SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,
         t2.t03lltt_ruc,t2.t03periodo,t2.t03f_presenta 
  FROM 
  (
    SELECT t03periodo,
           t03lltt_ruc,
           t03formulario,
           MAX(t03f_presenta) as t03f_presenta,
           MAX(t03nresumen) as t03nresumen,
           MAX(t03norden)  as t03norden
    FROM bddwestg.t03djcab
    WHERE t03formulario = '0601' 
    AND t03periodo BETWEEN '202201' and '202212'
    AND t03f_presenta <=DATE '2023-02-06'
    GROUP BY 1,2,3
  ) AS t1 
  INNER JOIN bddwestg.t03djcab t2 ON t2.t03periodo = t1.t03periodo 
  AND t2.t03lltt_ruc = t1.t03lltt_ruc
  AND t2.t03formulario = t1.t03formulario
  AND t2.t03f_presenta = t1.t03f_presenta
  AND t2.t03nresumen = t1.t03nresumen
  AND t2.t03norden = t1.t03norden
)
WITH DATA NO PRIMARY INDEX;



DROP TABLE bddwestg.tmp093168_kpigr9_periodos_compag;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr9_periodos_compag AS
(
  SELECT DISTINCT
    CASE WHEN x0.cod_tip_doc_ide = '06' 
         THEN x2.ddp_numruc
    ELSE  x3.dds_numruc END AS num_rucs,
      x0.num_ruc,
      x0.num_paq as num_nabono,
      x0.formulario as cod_formul,
      x0.norden as num_orden,
      x0.per_decla
  FROM bddwestg.t4583com_pag x0 
  INNER JOIN bddwestg.tmp093168_udjkpigr9 x1 
  ON  x1.t03lltt_ruc = x0.num_ruc
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  LEFT JOIN bddwestg.ddp x2 
  ON x0.num_doc_ide=x2.ddp_numruc AND x0.cod_tip_doc_ide='06'
  LEFT JOIN bddwestg.dds x3 
  ON x0.num_doc_ide=x3.dds_nrodoc AND cast(cast(x0.cod_tip_doc_ide AS int) as varchar(2))=x3.dds_docide
  WHERE x0.per_decla BETWEEN '202201' AND '202212' 
  AND x0.formulario = '0601'
  AND x0.ind_com_pag = 'D'
  AND x0.mto_retenido > 0
  AND  num_rucs IS NOT NULL
  ) WITH DATA NO PRIMARY INDEX;


DROP TABLE bddwestg.tmp093168_kpigr09_detcntpertr;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_detcntpertr
AS(
SELECT DISTINCT x0.num_rucs AS num_ruc_trab,
      x0.num_ruc as num_ruc_empl,
      x0.per_decla,
      x0.cod_formul,
      x0.num_orden,
      coalesce(x1.ind_presdj,0) as ind_presdj
FROM bddwestg.tmp093168_kpigr9_periodos_compag x0
LEFT JOIN bddwestg.tmp093168_kpiperindj x1 ON x0.num_rucs = x1.num_ruc
WHERE substr(x0.num_rucs,1,1) <>'2' or  x0.num_rucs in (select num_ruc from bddwestg.tmp093168_rucs20_incluir)
) WITH DATA NO PRIMARY INDEX ;




DROP TABLE bddwestg.tmp093168_kpigr09_detcntper1851;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_detcntper1851
AS(
SELECT  DISTINCT x0.num_ruc,x0.NUM_RUC_RET,x0.per_tri,x0.cod_for,x0.num_ord,
        coalesce(x1.ind_presdj,0) as ind_presdj
FROM bddwestg.t1851ret_rta x0
 INNER JOIN bddwestg.tmp093168_kpiperindj x1 ON x0.num_ruc = x1.num_ruc
WHERE  SUBSTR(x0.per_tri,1,4) = '2022'
   AND x0.cod_tri = '030402'
   AND x0.cod_for IN ('0621','0601')
   AND x0.mto_ret > 0
) WITH DATA NO PRIMARY INDEX ;


DROP TABLE bddwestg.tmp093168_kpigr09_detcntperfv;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_detcntperfv
AS(
SELECT DISTINCT x1.num_ruc,x0.num_doc,x0.per_mes,x0.num_formul,x0.num_ord,
        coalesce(x1.ind_presdj,0) as ind_presdj
FROM bddwestg.t12735cas130 x0
 INNER JOIN bddwestg.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
WHERE x0.cod_tip_doc = '06'
AND x0.mto_retenido > 0
) WITH DATA NO PRIMARY INDEX ;


DROP TABLE bddwestg.tmp093168_kpigr09_detcntpermdb;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_detcntpermdb
AS(
SELECT DISTINCT x1.num_ruc,x0.num_doc,x0.num_perimpreten,x0.cod_formul,x0.num_numorden,
    coalesce(x1.ind_presdj,0) as ind_presdj
FROM bddwestg.t12735cas130_mongodb x0
 INNER JOIN bddwestg.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
WHERE x0.cod_tipdoc = '06'
AND x0.num_mtoretenido > 0
) WITH DATA NO PRIMARY INDEX ;



/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE bddwestg.tmp093168_kpigr09_cnorigen;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_cnorigen AS
(
  SELECT ind_presdj,count(per_decla) as cant_per_origen
  FROM bddwestg.tmp093168_kpigr09_detcntpertr
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en T1851




DROP TABLE bddwestg.tmp093168_kpigr09_cnorigent1851;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_cnorigent1851 AS
(
  SELECT ind_presdj,count(per_tri) as cant_per_origent1851
  FROM bddwestg.tmp093168_kpigr09_detcntper1851
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

 *** Table has been created. 
 *** Total elapsed time was 1 second.


---------3. Conteo en FVirtual


DROP TABLE bddwestg.tmp093168_kpigr09_cndestino1;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_cndestino1 AS
(
  SELECT ind_presdj,count(per_mes) as cant_per_destino1
  FROM bddwestg.tmp093168_kpigr09_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------4. Conteo en MongoDB



DROP TABLE bddwestg.tmp093168_kpigr09_cndestino2 ;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr09_cndestino2 AS
(
  SELECT ind_presdj,count(num_perimpreten) as cant_per_destino2
  FROM bddwestg.tmp093168_kpigr09_detcntpermdb
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;



/********************INSERT EN TABLA FINAL***********************************/

  DELETE FROM bddwestg.T11908DETKPITRIBINT 
  WHERE COD_KPI='K009012022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO bddwestg.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
         'K009012022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
             case when x0.ind_presdj=0 then 
                      (select coalesce(sum(cant_per_origen),0) from bddwestg.tmp093168_kpigr09_cnorigen) 
            else 0 end as cant_origen,
             coalesce(x1.cant_per_origent1851,0) as cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
          from
          (
            select * from bddwestg.tmp093168_kpigr09_cnorigen
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      )  x0
      LEFT JOIN bddwestg.tmp093168_kpigr09_cnorigent1851 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;


  DELETE FROM bddwestg.T11908DETKPITRIBINT 
  WHERE COD_KPI='K009022022' AND FEC_CARGA=CURRENT_DATE;


  INSERT INTO bddwestg.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
          'K009022022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             case when x0.ind_presdj=0  then 
                      (select coalesce(sum(cant_per_destino2),0) from bddwestg.tmp093168_kpigr09_cndestino2)
             else 0 end AS cant_destino
      FROM 
      (
        select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
          from
          (
            select * from bddwestg.tmp093168_kpigr09_cndestino1
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN bddwestg.tmp093168_kpigr09_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;

/********************************************************************************/
/*************TRANSACCIONAL MENOS T1851*********************************/
  drop table bddwestg.tmp093168_dif_K009012022;
  CREATE MULTISET TABLE bddwestg.tmp093168_dif_K009012022 AS (
    SELECT 
          y0.num_ruc_trab,
          y0.num_ruc_empl,
          y0.per_decla,
          y0.cod_formul,
          y0.num_orden
    FROM
    (
      SELECT
            num_ruc_trab,
            num_ruc_empl,
            per_decla,
            cod_formul,
            num_orden
      FROM bddwestg.tmp093168_kpigr09_detcntpertr
      EXCEPT ALL
      SELECT
            NUM_RUC,
            NUM_RUC_RET,
            PER_TRI,
            COD_FOR,
            NUM_ORD
      FROM bddwestg.tmp093168_kpigr09_detcntper1851
     ) y0
   ) WITH DATA PRIMARY INDEX (num_ruc_trab,per_decla);


  LOCK ROW FOR ACCESS
  SELECT * FROM bddwestg.tmp093168_dif_K009012022
  ORDER BY num_ruc_trab,per_decla;
  


  /*******************FVIRTUAL MENOS MONGO*********************************/

 drop table bddwestg.tmp093168_dif_K009022022 ;
  CREATE MULTISET TABLE bddwestg.tmp093168_dif_K009022022 AS (
  SELECT 
      y0.num_ruc as num_ruc_trab,
        y0.num_doc as num_ruc_empl,
        y0.per_mes,
        y0.num_formul,
        y0.num_ord
  FROM
  (
      SELECT
            num_ruc,
            TRIM(num_doc) as num_doc,
            per_mes,
            num_formul,
            num_ord
      FROM  bddwestg.tmp093168_kpigr09_detcntperfv     
      EXCEPT ALL
      SELECT 
            num_ruc,
            TRIM(NUM_DOC),
            NUM_PERIMPRETEN,
            COD_FORMUL,
            NUM_NUMORDEN
      FROM bddwestg.tmp093168_kpigr09_detcntpermdb
  )  y0
  ) WITH DATA PRIMARY INDEX (num_ruc_trab,per_mes);

  LOCK ROW FOR ACCESS
  SELECT * FROM bddwestg.tmp093168_dif_K009022022
  ORDER BY num_ruc_trab,per_mes;
  
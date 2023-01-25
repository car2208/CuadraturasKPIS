
/**********Obtiene Última DJ Form 0601 ********************************/

DROP TABLE BDDWESTGD.tmp093168_udjkpigr3;

CREATE MULTISET TABLE BDDWESTGD.tmp093168_udjkpigr3 as
(
  SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,
         t2.t03lltt_ruc,t2.t03periodo,t2.t03f_presenta 
  FROM 
  (
    SELECT t03periodo,
           t03lltt_ruc,
           t03formulario,
           MAX(t03f_presenta) as t03f_presenta,
           MAX(t03nresumen) as t03nresumen 
    FROM BDDWESTGD.t03djcab_2
    WHERE t03formulario = '0601' 
    AND SUBSTR(t03periodo,1,4)=2022
    GROUP BY 1,2,3
  ) AS t1 
  INNER JOIN BDDWESTGD.t03djcab_2 t2 ON t2.t03periodo = t1.t03periodo 
  AND t2.t03lltt_ruc = t1.t03lltt_ruc
  AND t2.t03formulario = t1.t03formulario
  AND t2.t03f_presenta = t1.t03f_presenta
  AND t2.t03nresumen = t1.t03nresumen
)
WITH DATA NO PRIMARY INDEX;

/******************Obtiene periodos declarados en el PLAME***************************/

DROP TABLE BDDWESTGD.tmp093168_kpigr3_periodos_compag;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr3_periodos_compag AS
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
  FROM BDDWESTGD.t4583com_pag_2 x0 
  INNER JOIN BDDWESTGD.tmp093168_udjkpigr2 x1 
  ON  x1.t03lltt_ruc = x0.num_ruc
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  LEFT JOIN BDDWESTGD.ddp_2 x2 
  ON x0.num_doc_ide=x2.ddp_numruc AND x0.cod_tip_doc_ide='06'
  LEFT JOIN BDDWESTGD.dds_2 x3 
  ON x0.num_doc_ide=x3.dds_nrodoc AND cast(x0.cod_tip_doc_ide AS int)=x3.dds_docide
  WHERE SUBSTR(x0.per_decla,1,4)=2022
  AND x0.formulario = '0601'
  AND x0.ind_com_pag = 'D'
  AND x0.mto_servicio IS NOT NULL
  AND  num_rucs IS NOT NULL

) WITH DATA NO PRIMARY INDEX;


/**************************Obtiene Última DJ de Form 0616 ********************************/

DROP TABLE BDDWESTGD.tmp093168_udj_f616_kpigr3;

CREATE MULTISET TABLE BDDWESTGD.tmp093168_udj_f616_kpigr3 as
(
  SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,
         t2.t03lltt_ruc,t2.t03periodo,t2.t03f_presenta 
  FROM 
  (
  SELECT t03periodo,
         t03lltt_ruc,
         t03formulario,
         MAX(t03f_presenta) as t03f_presenta,
         MAX(t03nresumen) as t03nresumen 
  FROM BDDWESTGD.t03djcab_2
  WHERE t03formulario = '0616' 
  AND SUBSTR(t03periodo,1,4)=2022
  GROUP BY 1,2,3
  ) AS t1 
  INNER JOIN BDDWESTGD.t03djcab_2 t2 ON t2.t03periodo = t1.t03periodo 
  AND t2.t03lltt_ruc = t1.t03lltt_ruc
  AND t2.t03formulario = t1.t03formulario
  AND t2.t03f_presenta = t1.t03f_presenta
  AND t2.t03nresumen = t1.t03nresumen
)
WITH DATA NO PRIMARY INDEX;


/*************** Obtiene periodos declarados en F0616****************************************/

DROP TABLE BDDWESTGD.tmp093168_kpigr3_periodos_f0616;

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr3_periodos_f0616 as
(
  SELECT  x0.num_docide_dec,
          x0.num_docide_ret,
          x0.num_paq as num_nabono,
          x0.formulario as cod_formul,
          x0.norden as num_orden,
          x0.per_periodo 
  FROM BDDWESTGD.t1209f616rddet x0, BDDWESTGD.tmp093168_udj_f616_kpigr3 x1
  WHERE x0.tip_docide_dec = '6'
  AND SUBSTR(x0.per_periodo,1,4)=2022
  AND x0.formulario = '0616'
  AND x0.tip_cp = '99'
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  AND LENGTH(x0.num_docide_ret) = '11'
) WITH DATA NO PRIMARY INDEX;
;


/**************Union de Periodos Plame con Periodos f0616*********************************/

DROP TABLE BDDWESTGD.tmp093168_kpigr03_detcnt_tr;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr03_detcnt_tr AS
(
  
 SELECT num_docide_dec as num_ruc,
         num_docide_ret as num_docide_empl,
         num_nabono,
         cod_formul,
         num_orden,
         per_periodo as per_decla
  FROM BDDWESTGD.tmp093168_kpigr3_periodos_f0616
  UNION
  SELECT 
         num_rucs,
         num_ruc as num_docide_empl,
         num_nabono,
         cod_formul,
         num_orden,
         per_decla
  FROM BDDWESTGD.tmp093168_kpigr3_periodos_compag
)WITH DATA NO PRIMARY INDEX;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/

-------1. Detalle de Periodos  en transaccional


DROP TABLE BDDWESTGD.tmp093168_kpigr03_detcntpertr;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr03_detcntpertr
AS(
SELECT 
    DISTINCT x0.num_ruc,
    COALESCE(x1.ind_presdj,0) as ind_presdj,
        x0.num_docide_empl,
        x0.num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_decla
FROM BDDWESTGD.tmp093168_kpigr03_detcnt_tr x0
LEFT JOIN BDDWESTGD.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
) WITH DATA NO PRIMARY INDEX ; 


-------2. Detalle de Periodos en Archivo Personalizado Fvirtual


DROP TABLE BDDWESTGD.tmp093168_kpigr03_detcntperfv;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr03_detcntperfv
AS
(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc,
          x0.periodo
  FROM BDDWESTGD.T5376CAS108_2 x0
  LEFT JOIN BDDWESTGD.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  WHERE x0.tip_doc='06'
) WITH DATA NO PRIMARY INDEX ; 


-------3. Detalle de Periodos en Archivo Personalizado MongoDB

DROP TABLE BDDWESTGD.tmp093168_kpigr03_detcntpermdb;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr03_detcntpermdb
AS(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc, x0.num_perservicio
  FROM BDDWESTGD.T5376CAS108_MONGODB_2 x0
  LEFT JOIN BDDWESTGD.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  WHERE x0.COD_TIPDOC='06'
) WITH DATA NO PRIMARY INDEX ; 


/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTGD.tmp093168_kpigr03_cnorigen;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr03_cnorigen AS
(
  SELECT ind_presdj,count(per_decla) as cant_per_origen
  FROM BDDWESTGD.tmp093168_kpigr03_detcntpertr
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE BDDWESTGD.tmp093168_kpigr03_cndestino1;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr03_cndestino1 AS
(
  SELECT ind_presdj,count(periodo) as cant_per_destino1
  FROM BDDWESTGD.tmp093168_kpigr03_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE BDDWESTGD.tmp093168_kpigr03_cndestino2 ;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr03_cndestino2 AS
(
  SELECT ind_presdj,count(num_perservicio) as cant_per_destino2
  FROM BDDWESTGD.tmp093168_kpigr03_detcntpermdb
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/********************INSERT EN TABLA FINAL***********************************/

  DELETE FROM BDDWEDQD.TXXXXDETKPITRIBINT 
  WHERE COD_KPI='KPI0005022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWEDQD.TXXXXDETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  2022,
          z.ind_presdj,
         'KPI0005022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
             x0.cant_per_origen as cant_origen,
             coalesce(x1.cant_per_destino1,0) as cant_destino
      FROM BDDWESTGD.tmp093168_kpigr03_cnorigen x0
      LEFT JOIN BDDWESTGD.tmp093168_kpigr03_cndestino1 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;


  DELETE FROM BDDWEDQD.TXXXXDETKPITRIBINT 
  WHERE COD_KPI='KPI0006022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWEDQD.TXXXXDETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  2022,
          z.ind_presdj,
          'KPI0006022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             coalesce(x1.cant_per_destino2,0) AS cant_destino
      FROM BDDWESTGD.tmp093168_kpigr03_cndestino1 x0
      LEFT JOIN BDDWESTGD.tmp093168_kpigr03_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;
  
 
 SELECT 
    'KPI0005022' as cod_kpi,
      y0.num_ruc,
    y0.ind_presdj,
        y0.num_docide_empl,
        y0.num_nabono,
        y0.cod_formul,
        y0.num_orden,
        y0.per_decla
  FROM BDDWESTGD.tmp093168_kpigr03_detcntpertr y0
  INNER JOIN
  (
    SELECT DISTINCT num_ruc,ind_presdj,num_docide_empl,
                    SUBSTR(per_decla,5,2)||SUBSTR(per_decla,1,4) as per_decla
    FROM BDDWESTGD.tmp093168_kpigr03_detcntpertr
    EXCEPT ALL
    SELECT num_ruc,ind_presdj,num_doc,periodo 
    FROM BDDWESTGD.tmp093168_kpigr02_detcntperfv
  ) y1 
  ON y0.num_ruc=y1.num_ruc 
  AND y0.ind_presdj=y1.ind_presdj 
  AND y0.num_docide_empl=y1.num_docide_empl
  AND SUBSTR(y0.per_decla,5,2)||SUBSTR(y0.per_decla,1,4)=y1.per_decla;


  SELECT   'KPI0006022',
          y0.num_ruc,
          y0.ind_presdj,
        y0.num_doc,
        y0.periodo  
  FROM
  (
    SELECT num_ruc,ind_presdj,num_doc,periodo 
    FROM BDDWESTGD.tmp093168_kpigr03_detcntperfv
    EXCEPT ALL
    SELECT num_ruc,ind_presdj,num_doc,num_perservicio
    FROM BDDWESTGD.tmp093168_kpigr03_detcntpermdb
  ) y0;
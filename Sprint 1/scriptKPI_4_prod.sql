
/************Obtiene úlima DJ ***********************************************/

DROP TABLE BDDWESTG.tmp093168_udjkpigr4;

CREATE MULTISET TABLE BDDWESTG.tmp093168_udjkpigr4 as
(
SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,
     t2.t03lltt_ruc,t2.t03periodo,t2.t03f_presenta 
FROM 
(
SELECT  t03periodo,
        t03lltt_ruc,
        t03formulario,
        MAX(t03f_presenta) as t03f_presenta,
        MAX(t03nresumen) as t03nresumen,
        MAX(t03norden)  as t03norden 
FROM BDDWESTG.t03djcab
WHERE t03formulario = '0601' 
AND  t03periodo between '202201' and '202212'
AND t03f_presenta <= DATE '2022-10-27'
GROUP BY 1,2,3
) AS t1 
INNER JOIN BDDWESTG.t03djcab t2 ON t2.t03periodo = t1.t03periodo 
AND t2.t03lltt_ruc = t1.t03lltt_ruc
AND t2.t03formulario = t1.t03formulario
AND t2.t03f_presenta = t1.t03f_presenta
AND t2.t03nresumen = t1.t03nresumen
AND t2.t03norden = t1.t03norden
)
WITH DATA NO PRIMARY INDEX;

/***************************************************************************************/
---------Obtiene periodos de aportacion declarados en PLAME ----------------------------

DROP TABLE BDDWESTG.tmp093168_kpigr4_periodos_ctaind;

CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr4_periodos_ctaind as
(
  SELECT  DISTINCT 
        x2.dds_numruc as num_docide_aseg,
        x0.num_docide_empl,
        x0.num_paquete as num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM BDDWESTG.t727nctaind x0
INNER JOIN  BDDWESTG.tmp093168_udjkpigr4 x1 
ON  x1.t03lltt_ruc = x0.num_docide_empl
AND x1.t03nabono = x0.num_paquete
AND x1.t03formulario = x0.cod_formul 
AND x1.t03norden = x0.num_orden
INNER JOIN  BDDWESTG.dds x2
ON x0.num_docide_aseg=x2.dds_nrodoc 
AND cast(cast(x0.tip_docide_aseg as int) as varchar(3))=x2.dds_docide
WHERE x0.per_aporta between '202201' and '202212'
AND x0.cod_formul = '0601'
AND x0.cod_tributo = '030502'
AND x0.ind_exist_aseg IN ('6','8')
AND x0.tip_trabajador NOT IN ('23','24','26','35')
AND x0.mto_base_imp IS NOT NULL
AND x0.mto_aporta IS NOT NULL
)
WITH DATA NO PRIMARY INDEX;



DROP TABLE BDDWESTG.tmp093168_kpigr04_detcnt_tr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr04_detcnt_tr AS
(
   SELECT
        TRIM(num_docide_aseg) as num_ruc,
        TRIM(num_docide_empl) as num_docide_empl,
        num_nabono,
        cod_formul,
        num_orden,
        per_aporta
   FROM BDDWESTG.tmp093168_kpigr4_periodos_ctaind
)
WITH DATA NO PRIMARY INDEX;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/

-------1. Detalle de Periodos  en transaccional

DROP TABLE BDDWESTG.tmp093168_kpigr04_detcntpertr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr04_detcntpertr
AS(
SELECT 
    DISTINCT x0.num_ruc,
    COALESCE(x1.ind_presdj,0) as ind_presdj,
        x0.num_docide_empl,
        x0.num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM BDDWESTG.tmp093168_kpigr04_detcnt_tr x0
LEFT JOIN BDDWESTG.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
) WITH DATA NO PRIMARY INDEX ; 

-------2. Detalle de Periodos en Archivo Personalizado Fvirtual


DROP TABLE BDDWESTG.tmp093168_kpigr04_detcntperfv;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr04_detcntperfv
AS(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc,
          x0.periodo
  FROM BDDWESTG.t5377cas111 x0
  INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  WHERE x0.tip_doc = '06'
) WITH DATA NO PRIMARY INDEX ; 


-------3. Detalle de Periodos en Archivo Personalizado MongoDB

DROP TABLE BDDWESTG.tmp093168_kpigr04_detcntpermdb;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr04_detcntpermdb
AS(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc, x0.num_perservicio
  FROM BDDWESTG.T5377CAS111_MONGODB x0
  INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
  WHERE x0.COD_TIPDOC ='06'
) WITH DATA NO PRIMARY INDEX; 

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTG.tmp093168_kpigr04_cnorigen;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr04_cnorigen AS
(
  SELECT ind_presdj,count(per_aporta) as cant_per_origen
  FROM BDDWESTG.tmp093168_kpigr04_detcntpertr
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE BDDWESTG.tmp093168_kpigr04_cndestino1;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr04_cndestino1 AS
(
  SELECT ind_presdj,count(periodo) as cant_per_destino1
  FROM BDDWESTG.tmp093168_kpigr04_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE BDDWESTG.tmp093168_kpigr04_cndestino2 ;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr04_cndestino2 AS
(
  SELECT ind_presdj,count(num_perservicio) as cant_per_destino2
  FROM BDDWESTG.tmp093168_kpigr04_detcntpermdb
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/********************INSERT EN TABLA FINAL***********************************/

  INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
         'K004012022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
             x0.cant_per_origen as cant_origen,
             coalesce(x1.cant_per_destino1,0) as cant_destino
      FROM BDDWESTG.tmp093168_kpigr04_cnorigen x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr04_cndestino1 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;


  INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
          'K004022022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             coalesce(x1.cant_per_destino2,0) AS cant_destino
      FROM BDDWESTG.tmp093168_kpigr04_cndestino1 x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr04_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;

/********************************************************************************************/

 CREATE MULTISET TABLE BDDWESTG.DIF_K004012022
  AS
  (
  SELECT 
    'K004012022' as cod_kpi,
      y0.num_ruc,
    y0.ind_presdj,
        y0.num_docide_empl,
        y0.num_nabono,
        y0.cod_formul,
        y0.num_orden,
        y0.per_aporta
  FROM BDDWESTG.tmp093168_kpigr04_detcntpertr y0
  INNER JOIN
  (
    SELECT DISTINCT num_ruc,ind_presdj,num_docide_empl,
                    SUBSTR(per_aporta,5,2)||SUBSTR(per_aporta,1,4) as per_aporta
    FROM BDDWESTG.tmp093168_kpigr04_detcntpertr
    EXCEPT ALL
    SELECT num_ruc,ind_presdj,num_doc,periodo 
    FROM BDDWESTG.tmp093168_kpigr04_detcntperfv
  ) y1 
  ON y0.num_ruc=y1.num_ruc 
  AND y0.ind_presdj=y1.ind_presdj 
  AND y0.num_docide_empl=y1.num_docide_empl
  AND SUBSTR(y0.per_aporta,5,2)||SUBSTR(y0.per_aporta,1,4)=y1.per_aporta
)WITH DATA NO PRIMARY INDEX;


   CREATE MULTISET TABLE BDDWESTG.DIF_K004022022
  AS
  (
  SELECT   'K004022022' as cod_kpi,
          y0.num_ruc,
          y0.ind_presdj,
        y0.num_doc,
        y0.periodo  
  FROM
  (
    SELECT num_ruc,ind_presdj,num_doc,periodo 
    FROM BDDWESTG.tmp093168_kpigr04_detcntperfv
    EXCEPT ALL
    SELECT num_ruc,ind_presdj,num_doc,num_perservicio
    FROM BDDWESTG.tmp093168_kpigr04_detcntpermdb
  ) y0
  )WITH DATA NO PRIMARY INDEX;
--DROP TABLE BDDWESTG.tmpPilot093168_udjkpigr3;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr3_periodos_compag;
--DROP TABLE BDDWESTG.tmpPilot093168_udj_f616_kpigr3;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr3_periodos_f0616;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcnt_tr;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntpertr;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntperfv;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntpermdb;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_cnorigen;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_cndestino1;
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_cndestino2 ;
--DROP TABLE BDDWESTG.DIF_K003P12022;
--DROP TABLE BDDWESTG.DIF_K003P22022;

/**********Obtiene Última DJ Form 0601 ********************************/

--DROP TABLE BDDWESTG.tmpPilot093168_udjkpigr3;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_udjkpigr3 as
(

  SELECT t2.num_nabono as t03nabono,
        t2.num_orden as t03norden,
        t2.cod_formul as t03formulario,
        t2.num_ruc as  t03lltt_ruc,
         t2.cod_per as t03periodo,
         t2.fec_presenta as t03f_presenta 
FROM 
		(
			SELECT 
			    cod_per ,
				num_ruc ,
				cod_formul ,
				MAX(fec_presenta) as fec_presenta,
				MAX(num_resumen) as num_resumen,
				MAX(num_orden) as num_orden 
			FROM bddwetb.t8593djcab 
			WHERE cod_formul = '0601' 
			AND cod_per between '202201' and '202212'
			AND fec_presenta <= DATE '2023-02-19'
			GROUP BY 1,2,3
) t1			
  INNER JOIN bddwetb.t8593djcab t2 ON t2.cod_per = t1.cod_per 
  AND t2.num_ruc = t1.num_ruc
  AND t2.cod_formul = t1.cod_formul
  AND t2.fec_presenta = t1.fec_presenta
  AND t2.num_resumen = t1.num_resumen
  AND t2.num_orden = t1.num_orden
)
WITH DATA NO PRIMARY INDEX;

/******************Obtiene periodos declarados en el PLAME***************************/

--DROP TABLE BDDWESTG.tmpPilot093168_kpigr3_periodos_compag;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr3_periodos_compag AS
(

  SELECT DISTINCT
    CASE WHEN x0.cod_tip_doc_ide = '06' 
         THEN x2.num_ruc
    ELSE  x3.num_ruc END AS num_rucs,
      x0.num_ruc,
      x0.num_paq as num_nabono,
      x0.formulario as cod_formul,
      x0.norden as num_orden,
      x0.per_decla
  FROM BDDWESTG.t4583com_pag x0 
  INNER JOIN BDDWESTG.tmpPilot093168_udjkpigr3 x1 
  ON  x1.t03lltt_ruc = x0.num_ruc
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  LEFT JOIN bddwetb.T2018PadSunat x2 
  ON x0.num_doc_ide=x2.num_ruc AND x0.cod_tip_doc_ide='06' 
  and x2.fec_finvig=2000101 and x2.ind_delDWE ='0'
  LEFT JOIN bddwetb.T2018PadSunat x3 
  ON x0.num_doc_ide=x3.num_docidenti AND cast(x0.cod_tip_doc_ide AS int)=x3.cod_tipidenti 
  and x3.fec_finvig=2000101 and x3.ind_delDWE ='0'
  WHERE x0.per_decla BETWEEN '202201' AND '202212'
  AND x0.formulario = '0601'
  AND x0.ind_com_pag = 'D'
  AND x0.mto_servicio > 0
  AND  num_rucs IS NOT NULL

) WITH DATA NO PRIMARY INDEX;


/**************************Obtiene Última DJ de Form 0616 ********************************/

--DROP TABLE BDDWESTG.tmpPilot093168_udj_f616_kpigr3;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_udj_f616_kpigr3 as
(
  SELECT t2.num_nabono as t03nabono,
        t2.num_orden as t03norden,
        t2.cod_formul as t03formulario,
        t2.num_ruc as  t03lltt_ruc,
         t2.cod_per as t03periodo,
         t2.fec_presenta as t03f_presenta 
FROM 
		(
			SELECT 
			    cod_per ,
				num_ruc ,
				cod_formul ,
				MAX(fec_presenta) as fec_presenta,
				MAX(num_resumen) as num_resumen,
				MAX(num_orden) as num_orden 
			FROM bddwetb.t8593djcab
			WHERE cod_formul = '0616' 
			AND cod_per between '202201' and '202212'
			AND fec_presenta <= DATE '2023-02-19'
			GROUP BY 1,2,3
) t1			
  INNER JOIN bddwetb.t8593djcab t2 ON t2.cod_per = t1.cod_per 
  AND t2.num_ruc = t1.num_ruc
  AND t2.cod_formul = t1.cod_formul
  AND t2.fec_presenta = t1.fec_presenta
  AND t2.num_resumen = t1.num_resumen
  AND t2.num_orden = t1.num_orden
)
WITH DATA NO PRIMARY INDEX;


/*************** Obtiene periodos declarados en F0616****************************************/

--DROP TABLE BDDWESTG.tmpPilot093168_kpigr3_periodos_f0616;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr3_periodos_f0616 as
(
  SELECT  DISTINCT x0.num_docide_dec,
          x0.num_docide_ret,
          x0.num_paq as num_nabono,
          x0.formulario as cod_formul,
          x0.norden as num_orden,
          x0.per_periodo 
  FROM BDDWESTG.t1209f616rddet x0, BDDWESTG.tmpPilot093168_udj_f616_kpigr3 x1
  WHERE x0.tip_docide_dec = '6'
  AND x0.per_periodo BETWEEN '202201' AND '202212'
  AND x0.formulario = '0616'
  AND x0.tip_cp = '99'
  AND x1.t03nabono = x0.num_paq
  AND x1.t03formulario = x0.formulario 
  AND x1.t03norden = x0.norden
  AND LENGTH(TRIM(x0.num_docide_ret)) = '11'
) WITH DATA NO PRIMARY INDEX;
;


/**************Union de Periodos Plame con Periodos f0616*********************************/

--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcnt_tr;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr03_detcnt_tr AS
(
  
 SELECT  TRIM(num_docide_dec) as num_ruc,
         TRIM(num_docide_ret) as num_docide_empl,
         num_nabono,
         cod_formul,
         num_orden,
         per_periodo as per_decla
  FROM BDDWESTG.tmpPilot093168_kpigr3_periodos_f0616
  UNION
  SELECT 
         TRIM(num_rucs),
         TRIM(num_ruc) as num_docide_empl,
         num_nabono,
         cod_formul,
         num_orden,
         per_decla
  FROM BDDWESTG.tmpPilot093168_kpigr3_periodos_compag
)WITH DATA NO PRIMARY INDEX;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/

-------1. Detalle de Periodos  en transaccional


--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntpertr;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntpertr
AS(
SELECT 
    DISTINCT x0.num_ruc,
    COALESCE(x1.ind_presdj,0) as ind_presdj,
        x0.num_docide_empl,
        x0.num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_decla
FROM BDDWESTG.tmpPilot093168_kpigr03_detcnt_tr x0
LEFT JOIN BDDWESTG.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
INNER JOIN bddwetb.T2018PadSunat x2 ON x0.num_ruc=x2.num_ruc 
WHERE x2.cod_condomici = '1'  AND x2.cod_tipidenti IN ('1','2','3','4','5','7','8') 
AND (substr(x0.num_ruc,1,1) <>'2' OR  
     x0.num_ruc in (select num_ruc from BDDWESTG.tmp093168_rucs20_incluir)
     )
) WITH DATA NO PRIMARY INDEX ; 



-------2. Detalle de Periodos en Archivo Personalizado Fvirtual


--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntperfv;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntperfv
AS
(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc,
          x0.periodo
  FROM BDDWESTG.T5376CAS108 x0
  INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
) WITH DATA NO PRIMARY INDEX ; 


-------3. Detalle de Periodos en Archivo Personalizado MongoDB

--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntpermdb;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr03_detcntpermdb
AS(
  SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
          x0.num_doc, x0.num_perservicio
  FROM BDDWESTG.T5376CAS108_MONGODB x0
  INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
) WITH DATA NO PRIMARY INDEX ; 


/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_cnorigen;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr03_cnorigen AS
(
  SELECT ind_presdj,count(per_decla) as cant_per_origen
  FROM BDDWESTG.tmpPilot093168_kpigr03_detcntpertr
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_cndestino1;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr03_cndestino1 AS
(
  SELECT ind_presdj,count(periodo) as cant_per_destino1
  FROM BDDWESTG.tmpPilot093168_kpigr03_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
--DROP TABLE BDDWESTG.tmpPilot093168_kpigr03_cndestino2 ;
CREATE MULTISET TABLE BDDWESTG.tmpPilot093168_kpigr03_cndestino2 AS
(
  SELECT ind_presdj,count(num_perservicio) as cant_per_destino2
  FROM BDDWESTG.tmpPilot093168_kpigr03_detcntpermdb
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/********************INSERT EN TABLA FINAL***********************************/

  DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
  WHERE COD_KPI='K003P12022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
         'K003P12022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
              case when x0.ind_presdj=0 then 
                      (select coalesce(sum(cant_per_origen),0) from BDDWESTG.tmpPilot093168_kpigr03_cnorigen) 
              else 0 end as cant_origen,
             coalesce(x1.cant_per_destino1,0) as cant_destino
      FROM (
          select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
          from
          (
            select * from BDDWESTG.tmpPilot093168_kpigr03_cnorigen
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1

      ) x0
      LEFT JOIN BDDWESTG.tmpPilot093168_kpigr03_cndestino1 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;


  DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
  WHERE COD_KPI='K003P22022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
          'K003P22022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             case when x0.ind_presdj=0  then 
                      (select coalesce(sum(cant_per_destino2),0) from BDDWESTG.tmpPilot093168_kpigr03_cndestino2)
             else 0 end AS cant_destino
      FROM (
          select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
          from
          (
            select * from BDDWESTG.tmpPilot093168_kpigr03_cndestino1
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1


      ) x0
      LEFT JOIN BDDWESTG.tmpPilot093168_kpigr03_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;
  
/***************************************************************************************/
 
DROP TABLE BDDWESTG.DIF_K003P12022;
  CREATE MULTISET TABLE BDDWESTG.DIF_K003P12022
  AS
  (
    SELECT 
        y0.num_ruc as num_ruc_trab,
	        y0.num_docide_empl as num_ruc_empl,
          y0.num_nabono,
          y0.cod_formul,
          y0.num_orden,
          y0.per_decla as per_dif
    FROM BDDWESTG.tmpPilot093168_kpigr03_detcntpertr y0
    INNER JOIN
    (
      SELECT DISTINCT num_ruc,num_docide_empl,
                      SUBSTR(per_decla,5,2)||SUBSTR(per_decla,1,4) as per_decla
      FROM BDDWESTG.tmpPilot093168_kpigr03_detcntpertr
      EXCEPT ALL
      SELECT num_ruc,num_doc,periodo 
      FROM BDDWESTG.tmpPilot093168_kpigr03_detcntperfv
    ) y1 
    ON y0.num_ruc=y1.num_ruc 
    AND y0.num_docide_empl=y1.num_docide_empl
    AND SUBSTR(y0.per_decla,5,2)||SUBSTR(y0.per_decla,1,4)=y1.per_decla
  )WITH DATA NO PRIMARY INDEX;


 DROP TABLE BDDWESTG.DIF_K003P22022;
  CREATE MULTISET TABLE BDDWESTG.DIF_K003P22022
  AS
  (
    SELECT  
          y0.num_ruc as num_ruc_trab,
          y0.num_doc as num_ruc_empl,
          y0.periodo as per_dif 
    FROM
    (
      SELECT num_ruc,num_doc,periodo 
      FROM BDDWESTG.tmpPilot093168_kpigr03_detcntperfv
      EXCEPT ALL
      SELECT num_ruc,num_doc,num_perservicio
      FROM BDDWESTG.tmpPilot093168_kpigr03_detcntpermdb
    ) y0
  )WITH DATA NO PRIMARY INDEX;

LOCK ROW FOR ACCESS
SELECT * FROM BDDWESTG.DIF_K003P12022 ORDER BY 1,6;

LOCK ROW FOR ACCESS
SELECT * FROM BDDWESTG.DIF_K003P22022 ORDER BY 1,3;s
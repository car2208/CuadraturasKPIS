
 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1;
 --t_origen_01
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1 as    
 (
  SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
    FROM BDDWESTG.CRT
  WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
    AND crt_codtri = '030501'
    AND crt_indaju = '0'
    AND crt_indpag IN (1,5)
    AND crt_fecpag <= CAST('2023-02-16' AS DATE FORMAT 'YYYY-MM-DD')
    AND crt_estado <> '02'
  UNION
  SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
    FROM BDDWESTG.CRT
  WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
    AND crt_codtri = '030501'                                      
    AND crt_tiptra = '2962'
    AND crt_indaju = '1'
    AND crt_indpag IN (1,5)
    AND crt_fecpag <= CAST('2023-02-16' AS DATE FORMAT 'YYYY-MM-DD')
    AND crt_estado <> '02'                
 ) WITH DATA NO PRIMARY INDEX;


 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_2;
 --t_origen_02
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_2 as    
 (
   SELECT crt_numruc numruc, crt_perpag perpag, 1648 formul, crt_docori numdoc 
    FROM BDDWESTG.CRT
   WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
     AND crt_codtri = '030501'
     AND crt_tiptra = '1472'
     AND crt_indaju = '1'
     AND crt_imptri > 0
     AND crt_fecpag <= CAST('2023-02-16' AS DATE FORMAT 'YYYY-MM-DD')
 ) WITH DATA NO PRIMARY INDEX;


  INSERT INTO BDDWESTG.TMP_KPI07_SIRATPRICO_1
  SELECT numruc, perpag, formul, numdoc  
   FROM BDDWESTG.TMP_KPI07_SIRATPRICO_2 a, BDDWESTG.cab_pre_res b
  WHERE b.num_res = a.numdoc
   AND b.cod_tip_doc = '023000'
   AND b.ind_est_pre = '1'
   AND b.ind_eta_pre = '2';

 
 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1651;
  --t_1651
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1651 as    
 (
  SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, b.num_doc_ori numdoc  
  from BDDWESTG.TMP_KPI07_SIRATPRICO_1 a, BDDWESTG.t1651sol_comp b
  WHERE a.numruc=b.num_ruc  
  AND b.ind_con_com IN ('3','4','5')
  AND b.cod_eta_sol IN ('01','02','03')
  AND b. cod_tri ='030501'
 ) WITH DATA NO PRIMARY INDEX;


 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_06;
 --t_origen_06
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_06 as    
 (

  SELECT a.numruc, a.perpag, a.formul, a.numdoc 
  FROM BDDWESTG.TMP_KPI07_SIRATPRICO_1 a
  LEFT JOIN BDDWESTG.TMP_KPI07_SIRATPRICO_1651 b
  ON a.numruc=b.numruc  and a.formul=b.formul and a.numdoc= b.numdoc
  WHERE b.numruc  IS NULL
  AND b.formul IS NULL
  AND b.numdoc IS NULL
 ) WITH DATA NO PRIMARY INDEX;



 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_tdev;
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_tdev as    --t_dev
 (
  SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_aso formul, b.num_doc_aso numdoc
  from BDDWESTG.TMP_KPI07_SIRATPRICO_1 a, BDDWESTG.devoluciones b
  WHERE a.numruc=b.num_ruc  
  AND b.cod_tip_sol = '02'
  AND b.ind_est_dev IN ('0','3')
  AND b.ind_res_dev IN ('0','F')
 ) WITH DATA NO PRIMARY INDEX;

  
--devoluciones 

 INSERT INTO BDDWESTG.TMP_KPI07_SIRATPRICO_06
 SELECT a.numruc, a.perpag, a.formul, a.numdoc 
 FROM BDDWESTG.TMP_KPI07_SIRATPRICO_1 a
 LEFT JOIN BDDWESTG.TMP_KPI07_SIRATPRICO_tdev b
 ON a.numruc=b.numruc  and a.formul=b.formul  and a.numdoc= b.numdoc
 WHERE b.numruc    IS NULL
 AND b.formul  IS NULL
 AND b.numdoc IS NULL;


 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_treimp;
 --t_reimp
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_treimp as    
 (  
  SELECT  a.numruc, a.perpag, b.cod_for formul, b.num_doc numdoc 
  from BDDWESTG.TMP_KPI07_SIRATPRICO_1 a, BDDWESTG.t869rei_cab b
  WHERE a.numruc=b.num_ruc  
  AND b.cod_for_rei ='4715'
  AND b.ind_aplica = '0'
  AND b.ind_motivo NOT IN ('0','9')
 ) WITH DATA NO PRIMARY INDEX;


  INSERT INTO BDDWESTG.TMP_KPI07_SIRATPRICO_06   
  SELECT a.numruc, a.perpag, a.formul, a.numdoc 
  FROM BDDWESTG.TMP_KPI07_SIRATPRICO_1 a
  LEFT JOIN BDDWESTG.TMP_KPI07_SIRATPRICO_treimp b
  ON a.numruc=b.numruc AND a.formul=b.formul and a.numdoc= b.numdoc
  WHERE b.formul IS NULL
  AND b.numdoc IS NULL;

-- Compensaciones a valores (crt):

 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_05;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_05 as    --t_origen_05
 ( 
 SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc 
   FROM BDDWESTG.t3386doc_deu_com a, BDDWESTG.cab_pre_res b
  WHERE a.cod_tri_deu = '030501'
    AND a.num_pre_res = b.num_pre_res
    AND b.ind_est_pre = '1'
    AND b.ind_eta_pre = '2'
    AND a.ind_tip_deu = '01'
    AND a.cod_tip_cal IN ('023001', '023002')
 ) WITH DATA NO PRIMARY INDEX;


 INSERT INTO BDDWESTG.TMP_KPI07_SIRATPRICO_06
 SELECT a.num_ruc numruc, per_tri_des perpag,a.cod_for,a.nro_orden
 FROM BDDWESTG.t1651sol_comp a ,BDDWESTG.TMP_KPI07_SIRATPRICO_05 b
 WHERE cod_for = '1648'
   AND a.nro_orden = b.numdoc 
   AND a.num_ruc = b.numruc 
   AND a.per_tri_des IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212');


 DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO;
 --t_origen_03
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO as    
 ( 

  SELECT DISTINCT numruc, perpag,formul ,numdoc 
   FROM BDDWESTG.TMP_KPI07_SIRATPRICO_06
 ) WITH DATA NO PRIMARY INDEX;


--- RESULTADO : TABLA BDDWESTG.TMP_KPI07_SIRATPRICO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT PRICO   	

/******************************* MEPECO SIRAT *********************************/
 -- PAGOS DDJJ 0616

 DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_01;
 --t_origen_01
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_01 
 AS(

   SELECT crt_numruc AS numruc, crt_perpag AS perpag, crt_formul AS formul,
          crt_ndocpa as numdoc
    FROM BDDWESTG.CRT
   WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
     AND crt_codtri = '030501'
     AND crt_formul NOT IN (1083,1683,116,616)
     AND crt_indaju = '0'
     AND crt_indpag IN (1,5)
     AND crt_estado <> '02'
     AND crt_imptri > 0
     AND crt_fecpag <= CAST('2023-02-16' AS DATE FORMAT 'YYYY-MM-DD')
   GROUP BY 1,2,3,4

 ) WITH DATA NO PRIMARY INDEX ;

 -- Exclusiones pago en proceso de compensacion 			

 DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_1651;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_1651 as    --t_1651
 (
  SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, b.num_doc_ori numdoc  
  from BDDWESTG.TMP_KPI07_SIRATMEPECO_01 a, BDDWESTG.t1651sol_comp b
  WHERE a.numruc=b.num_ruc  
  AND b.ind_con_com IN ('3','4','5')
  AND b.cod_eta_sol IN ('01','02','03')
  AND b. cod_tri ='030501'
 ) WITH DATA NO PRIMARY INDEX;


 --compensaciones

 
 DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_06;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_06 as    --t_origen_06
 (

  SELECT a.numruc, a.perpag, a.formul, a.numdoc 
  FROM BDDWESTG.TMP_KPI07_SIRATMEPECO_01 a
  LEFT JOIN BDDWESTG.TMP_KPI07_SIRATMEPECO_1651 b
  ON a.numruc=b.numruc  and a.formul=b.formul and a.numdoc= b.numdoc
  WHERE b.numruc  IS NULL
  AND b.formul IS NULL
  AND b.numdoc IS NULL
 ) WITH DATA NO PRIMARY INDEX;

 
 DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_tdev;
 --t_dev
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_tdev as    
 (
  SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_aso formul, b.num_doc_aso numdoc
  from BDDWESTG.TMP_KPI07_SIRATMEPECO_01 a, BDDWESTG.devoluciones b
  WHERE a.numruc=b.num_ruc  
  AND b.cod_tip_sol = '02'
  AND b.ind_est_dev IN ('0','3')
  AND b.ind_res_dev IN ('0','F')
 ) WITH DATA NO PRIMARY INDEX;

 --devoluciones 
  
  INSERT INTO BDDWESTG.TMP_KPI07_SIRATMEPECO_06
  SELECT a.numruc, a.perpag, a.formul, a.numdoc 
  FROM BDDWESTG.TMP_KPI07_SIRATMEPECO_01 a
  LEFT JOIN BDDWESTG.TMP_KPI07_SIRATMEPECO_tdev b
  ON a.numruc=b.numruc  and a.formul=b.formul  and a.numdoc= b.numdoc
  WHERE b.numruc IS NULL
  AND b.formul IS NULL
  AND b.numdoc IS NULL;
    
  --Exclusiones Pago en Proceso de reimputaci�n

  DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_treimp;
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_treimp as    --t_reimp
  (  
  
   SELECT  a.numruc, a.perpag, b.cod_for formul, b.num_doc numdoc
   from BDDWESTG.TMP_KPI07_SIRATMEPECO_01 a, BDDWESTG.t869rei_cab b
   WHERE a.numruc=b.num_ruc  
   AND b.cod_for_rei ='4715'
   AND b.ind_aplica = '0'
   AND b.ind_motivo NOT IN ('0','9')
   
  ) WITH DATA NO PRIMARY INDEX;

  
   INSERT INTO BDDWESTG.TMP_KPI07_SIRATMEPECO_06   
   SELECT a.numruc, a.perpag, a.formul, a.numdoc 
   FROM BDDWESTG.TMP_KPI07_SIRATMEPECO_01 a
   LEFT JOIN BDDWESTG.TMP_KPI07_SIRATMEPECO_treimp b
   ON a.numruc=b.numruc AND a.formul=b.formul and a.numdoc= b.numdoc
   WHERE b.formul IS NULL
   AND b.numdoc IS NULL
   AND b.numruc IS NULL;


 -- COMPENSACIONES CRT

 DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_02;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_02 as    --t_origen_02
 (  
  SELECT crt_numruc AS numruc, crt_perpag AS perpag, 1648 AS formul, crt_docori AS numdoc
  FROM BDDWESTG.CRT
  WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
    AND crt_codtri = '030501'
    AND crt_tiptra = '1472'
    AND crt_indaju = '1'
    AND crt_imptri > 0
    AND crt_fecpag <= CAST('2023-02-16' AS DATE FORMAT 'YYYY-MM-DD')
 ) WITH DATA NO PRIMARY INDEX;

 INSERT INTO  BDDWESTG.TMP_KPI07_SIRATMEPECO_01
 SELECT numruc, perpag, formul, numdoc  
  FROM BDDWESTG.TMP_KPI07_SIRATMEPECO_02 a, BDDWESTG.cab_pre_res b
 WHERE b.num_res = a.numdoc
  AND b.cod_tip_doc = '023000'
  AND b.ind_est_pre = '1'
  AND b.ind_eta_pre = '2';


 -- COMPENSACIONES A VALORES (CRT)


 DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_05;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_05 as    --t_origen_05
 (  
  SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc
  FROM BDDWESTG.t3386doc_deu_com a, BDDWESTG.cab_pre_res b
  WHERE a.cod_tri_deu = '030501'
  AND a.num_pre_res = b.num_pre_res
  AND b.ind_est_pre = '1'
  AND b.ind_eta_pre = '2'
  AND a.ind_tip_deu = '01'
  AND a.cod_tip_cal IN ('023001','023002')
        
 ) WITH DATA NO PRIMARY INDEX;


 INSERT INTO  BDDWESTG.TMP_KPI07_SIRATMEPECO_01
 SELECT a.num_ruc numruc, a.per_tri_des perpag,a.cod_for,a.nro_orden
 FROM BDDWESTG.t1651sol_comp a , BDDWESTG.TMP_KPI07_SIRATMEPECO_05 b
 WHERE a.cod_for = '1648'
 AND a.nro_orden = b.numdoc 
 AND a.num_ruc = b.numruc 
 AND a.per_tri_des IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212');


 DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO as    --t_origen_03
 ( 

  SELECT DISTINCT numruc,perpag,formul ,numdoc
   FROM BDDWESTG.TMP_KPI07_SIRATMEPECO_01
 ) WITH DATA NO PRIMARY INDEX;

     
-- RESULTADO : TABLA BDDWESTG.TMP_KPI07_SIRATMEPECO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT MEPECO
/**************************FIN MEPECO SIRAT ****************************************/

/***********************************************************************************************************************/ 
/***********************************************************************************************************************/
-------------------------PAGOS DIRECTOS EN TRANSACCIONAL PRICO Y MEPECO--------------------------------------------------


 DROP TABLE BDDWESTG.tmp093168_kpigr07_detcntpertr;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr07_detcntpertr  
 AS(
        SELECT
         x0.numruc,x0.perpag as periodo,x0.formul,x0.numdoc as norden,
         coalesce(x1.ind_presdj,0) as ind_presdj
  FROM(
            SELECT a.numruc, a.perpag,a.formul,a.numdoc 
      FROM BDDWESTG.TMP_KPI07_SIRATPRICO a , BDDWESTG.DDP_DEPEN b 
      WHERE a.numruc=b.ddp_numruc 
      UNION 
      SELECT a.numruc,a.perpag,a.formul ,a.numdoc 
      FROM BDDWESTG.TMP_KPI07_SIRATMEPECO a , BDDWESTG.DDP_DEPEN b 
      WHERE a.numruc=b.ddp_numruc
          ) x0
          INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.numruc = x1.num_ruc  
 ) WITH DATA NO PRIMARY INDEX ;


--------------------------PAGOS DIRECTOS  EN  FVIRTUAL------------------------------------------------------


 DROP TABLE BDDWESTG.tmp093168_kpigr07_detcntperfv;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr07_detcntperfv
 AS( 
 SELECT DISTINCT x1.num_ruc,
     SUBSTR(x0.periodo,3,4)||SUBSTR(x0.periodo,1,2) as periodo,
     CAST(x0.num_formul AS smallint) as cod_formul,
     CAST(x0.num_ordope AS BIGINT) as num_ordope,
     coalesce(x1.ind_presdj,0) as ind_presdj
 FROM BDDWESTG.T5410CAS128 x0
 INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
 ) WITH DATA NO PRIMARY INDEX;

-----------------------------------------------------------------------------------------------------------
-------------------------PAGOS DIRECTOS  EN  MONGOBB------------------------------------------------------

 DROP TABLE BDDWESTG.tmp093168_kpigr07_detcntpermdb;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr07_detcntpermdb
    AS( 

 SELECT 
    x1.num_ruc,
       substr(num_perpago,3,4)||substr(num_perpago,1,2) as periodo,
       cast(x0.cod_formul as smallint) as cod_formul,
       x0.num_numorden as num_ordope,
       coalesce(x1.ind_presdj,0) as ind_presdj
 FROM BDDWESTG.T5410CAS128_mongodb x0
 INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
 ) WITH DATA NO PRIMARY INDEX;


/**************************************************************************************/
/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

  DROP TABLE BDDWESTG.tmp093168_kpigr07_cnorigen;
  CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr07_cnorigen AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_origen
    FROM BDDWESTG.tmp093168_kpigr07_detcntpertr
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;


---------2. Conteo en FVirtual

 
  DROP TABLE BDDWESTG.tmp093168_kpigr07_cndestino1;
  CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr07_cndestino1 AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_destino1
    FROM BDDWESTG.tmp093168_kpigr07_detcntperfv
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB

  DROP TABLE BDDWESTG.tmp093168_kpigr07_cndestino2;
  CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr07_cndestino2 AS
  (
    SELECT ind_presdj,count(periodo) as cant_per_destino2
    FROM BDDWESTG.tmp093168_kpigr07_detcntpermdb
    GROUP BY 1
  ) WITH DATA NO PRIMARY INDEX;


/************************ INSERTA CONTEOS A TABLAS DE DETALLE **************************/

 DELETE FROM BDDWESTG.T11908DETKPITRIBINT  
  WHERE COD_KPI='K007012022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
         'K007012022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
             case when x0.ind_presdj=0 then (select coalesce(sum(cant_per_origen),0) from BDDWESTG.tmp093168_kpigr07_cnorigen) else 0 end as cant_origen,
             coalesce(x1.cant_per_destino1,0) as cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
          from
          (
            select * from BDDWESTG.tmp093168_kpigr07_cnorigen
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr07_cndestino1 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;

  DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
  WHERE COD_KPI='K007022022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWESTG.T11908DETKPITRIBINT  
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
          'K007022022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             case when x0.ind_presdj=0  then (select coalesce(sum(cant_per_destino2),0) from BDDWESTG.tmp093168_kpigr07_cndestino2) else 0 end AS cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
          from
          (
            select * from BDDWESTG.tmp093168_kpigr07_cndestino1
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr07_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 

  
  DROP TABLE BDDWESTG.tmp093168_dif_K007012022 ;
  CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K007012022 AS (
       SELECT DISTINCT 
    y0.numruc as num_ruc_trab,y0.periodo,y0.formul,y0.norden
   FROM (
    SELECT
              numruc,periodo,formul,norden  
    FROM BDDWESTG.tmp093168_kpigr07_detcntpertr
    EXCEPT ALL
    SELECT 
             num_ruc,periodo,cod_formul,num_ordope
          FROM BDDWESTG.tmp093168_kpigr07_detcntperfv
   ) y0
   ) WITH DATA NO PRIMARY INDEX;


  DROP TABLE BDDWESTG.tmp093168_dif_K007022022 ;
   CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K007022022 AS (
   SELECT DISTINCT 
    y0.num_ruc as num_ruc_trab,y0.periodo,y0.cod_formul,y0.num_ordope 
   FROM (
       SELECT 
             num_ruc,periodo,cod_formul,num_ordope
          FROM BDDWESTG.tmp093168_kpigr07_detcntperfv
    EXCEPT ALL
    SELECT 
             num_ruc,periodo,cod_formul,num_ordope
          FROM BDDWESTG.tmp093168_kpigr07_detcntpermdb
   ) y0
      ) WITH DATA NO PRIMARY INDEX;

    
 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K007012022 
 ORDER BY num_ruc_trab,periodo;

 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K007022022
 ORDER BY num_ruc_trab,periodo;


/*********************** FIN INSERTA CONTEOS A TABLAS DE DETALLE **********************/
/************************ BORRA TABLAS TEMPORALES **********************/

  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_2;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1651;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_06;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_tdev;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_treimp;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_05;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_01;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_1651;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_06;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_tdev;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_treimp;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_02;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_05;
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO;
  --DROP TABLE BDDWESTG.tmp093168_kpigr07_detcntpertr;
  --DROP TABLE BDDWESTG.tmp093168_kpigr07_detcntperfv;
  --DROP TABLE BDDWESTG.tmp093168_kpigr07_detcntpermdb;
  --DROP TABLE BDDWESTG.tmp093168_kpigr07_cnorigen;
  --DROP TABLE BDDWESTG.tmp093168_kpigr07_cndestino1;
  --DROP TABLE BDDWESTG.tmp093168_kpigr07_cndestino2;


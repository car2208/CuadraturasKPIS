/* ---- INICIO PASO1 EXTRAE PAGOS SIRAT 
/******************************* PRICO *********************************/

  
  /**** SIRAT PRICO ***/
  
  -- PAGO DIRECTO DE 4TA
   

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_10;
 --t_origen_10
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_10 as   
 (
  
  SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
    FROM BDDWESTG.CRT
  WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
    AND crt_codtri = '030401'
    AND crt_indaju = '0'
    AND crt_indpag IN (1,5)
    AND crt_fecpag <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD')
    AND crt_estado <> '02'
  UNION
  SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
    FROM BDDWESTG.CRT
  WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
    AND crt_codtri = '030401'                                      
    AND crt_tiptra = '2962'
    AND crt_indaju = '1'
    AND crt_indpag IN (1,5)
    AND crt_fecpag <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD')
    AND crt_estado <> '02'

 ) WITH DATA NO PRIMARY INDEX;

 
 -- FIN PAGO DIRECTO 4TA
 -- PAGO BOLETAS - no se considera boletas en proceso de compensacion

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_1651;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_1651 as    
 (
  SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, b.num_doc_ori numdoc  
  from BDDWESTG.TMP_KPI06_SIRATPRICO_10 a, BDDWESTG.t1651sol_comp b
  WHERE a.numruc = b.num_ruc
  AND a.formul = b.cod_for_ori
  AND a.norden = b.num_doc_ori 
  AND b.ind_con_com IN ('3','4','5')
  AND b.cod_eta_sol IN ('01','02','03')
  AND b.cod_tri ='030401'
 ) WITH DATA NO PRIMARY INDEX;

 
 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_09;
  --t_origen_09
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_09 as    
 (

  SELECT  a.numruc numruc, a.perpag perpag, a.formul, a.norden  
  from BDDWESTG.TMP_KPI06_SIRATPRICO_10 a 
  LEFT JOIN BDDWESTG.TMP_KPI06_SIRATPRICO_1651 b ON a.numruc = b.numruc 
   AND a.formul = b.formul
   AND a.norden = b.numdoc
  WHERE b.numruc is null
  AND b.formul is null
  AND b.numdoc is null 

  
 ) WITH DATA NO PRIMARY INDEX;

 
 --- Pago Boletas - No se considera boletas en proceso de devoluci�n:
 
  DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_tdevo;
  --t_devo
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_tdevo as    
 (
 
  SELECT b.num_ruc numruc, b.cod_for_aso formul, b.num_doc_aso norden
  FROM BDDWESTG.TMP_KPI06_SIRATPRICO_09 a, BDDWESTG.devoluciones b
  WHERE a.numruc = b.num_ruc
  AND a.formul = b.cod_for_aso
  AND a.norden = b.num_doc_aso 
  AND b.cod_tip_sol = '02'
  AND b.ind_est_dev IN ('0','3')
  AND b.ind_res_dev IN ('0','F')
 
 ) WITH DATA NO PRIMARY INDEX;

 
 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_08;
 --t_origen_08
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_08 as    
 (
 
  SELECT a.numruc, a.perpag, a.formul, a.norden 
  FROM BDDWESTG.TMP_KPI06_SIRATPRICO_09 a 
  LEFT JOIN BDDWESTG.TMP_KPI06_SIRATPRICO_tdevo b 
   ON a.numruc = b.numruc 
   AND a.formul = b.formul 
   AND a.norden = b.norden
  WHERE b.numruc  IS NULL
  AND b.formul IS NULL
  AND b.norden IS NULL

 ) WITH DATA NO PRIMARY INDEX;

 
 --- Pago Boletas � No se considera boletas en proceso de reimputaci�n:

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_t869;
 --t_869
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_t869 as    
 (
 
  SELECT b.num_ruc numruc, b.cod_for formul, b.num_doc norden
  FROM BDDWESTG.TMP_KPI06_SIRATPRICO_08 a, BDDWESTG.t869rei_cab b
  WHERE a.numruc = b.num_ruc
  AND a.formul = b.cod_for
  AND a.norden = b.num_doc 
  AND b.cod_for_rei = '4715'
  AND b.ind_aplica = '0'
  AND b.ind_motivo NOT IN ('0','9')
 
 ) WITH DATA NO PRIMARY INDEX;

 
 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_01;
 --t_origen_01
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_01 as    
 (
 
  SELECT a.numruc, a.perpag ,a.formul ,a.norden 
  FROM BDDWESTG.TMP_KPI06_SIRATPRICO_08 a LEFT JOIN BDDWESTG.TMP_KPI06_SIRATPRICO_t869 b 
  ON a.numruc = b.numruc AND a.formul = b.formul AND a.norden = b.norden
  WHERE b.numruc  IS NULL
  AND b.formul IS NULL
  AND b.norden IS NULL
 
 ) WITH DATA NO PRIMARY INDEX;

 
 --Saldo a favor aplicado (hsf):
  
  INSERT INTO BDDWESTG.TMP_KPI06_SIRATPRICO_01
  SELECT hsf_numruc numruc, hsf_perpag perpag,hsf_formul,hsf_numdoc
  FROM BDDWESTG.HSF
  WHERE hsf_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
  AND hsf_codtri = '030401'
  AND hsf_tiptra = '1041'
  AND hsf_tipcta = '01'
  AND hsf_fecsaf <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD');

 
  --  Otros cr�ditos de ley (dbt/doc):

  INSERT INTO BDDWESTG.TMP_KPI06_SIRATPRICO_01
  SELECT a.dbt_numruc numruc, a.dbt_perpag perpag,
               a.dbt_formul,
               cast(a.dbt_numdoc as integer) as dbt_numdoc 
  FROM BDDWESTG.DBT a,BDDWESTG.DOC b
  WHERE a.dbt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
  AND dbt_codtri = '030401'
  AND dbt_tiptra = 1011
  AND dbt_indrec = 0
  AND dbt_fecdoc <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD')
  AND doc_formul = dbt_formul
  AND doc_numdoc = dbt_numdoc
  AND doc_codcas = 347
  AND doc_valdec > 0;

   
 -- Compensaciones
 
  INSERT INTO BDDWESTG.TMP_KPI06_SIRATPRICO_01
  SELECT  dbt_numruc numruc, dbt_perpag perpag,
                db2_formul,
                cast(db2_numdoc as integer) as db2_numdoc
  FROM BDDWESTG.DB2, BDDWESTG.DBT
  WHERE dbt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
  AND dbt_codtri = '030401'
  AND dbt_tiptra = 1011
  AND dbt_fecdoc <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD')
  AND dbt_formul = db2_formul
  AND dbt_numdoc = db2_numdoc
  AND dbt_codtri = db2_codtri
  AND db2_compen > 0
  UNION
  SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
  FROM BDDWESTG.CRT
  WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
  AND crt_codtri = '030401'  
  AND crt_tiptra = '1272'
  AND crt_indaju = '1'
  AND crt_imptri > 0
  AND crt_fecpag <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD');

 
 -- BLOQUE05

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_06;
 --t_origen_06
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_06 as    
 ( 
  SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc 
  FROM BDDWESTG.t3386doc_deu_com a, BDDWESTG.cab_pre_res b
  WHERE a.cod_tri_deu = '030401'
  AND a.num_pre_res = b.num_pre_res
  AND b.ind_est_pre = '1'
  AND b.ind_eta_pre = '2'
  AND a.ind_tip_deu = '01'
  AND a.cod_tip_cal IN ('023001', '023002')
 ) WITH DATA NO PRIMARY INDEX;


  INSERT INTO BDDWESTG.TMP_KPI06_SIRATPRICO_01
  SELECT a.num_ruc numruc, per_tri_des perpag,a.cod_for,a.nro_orden
  FROM BDDWESTG.t1651sol_comp a,BDDWESTG.TMP_KPI06_SIRATPRICO_06 b
  WHERE a.cod_for = '1648'
  AND a.nro_orden = b.numdoc 
  AND a.num_ruc = b.numruc 
  AND per_tri_des IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212');


 -- depura duplicados

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO;
 --t_origen_02
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO as    
 ( 

  SELECT DISTINCT numruc, perpag,formul ,norden 
   FROM BDDWESTG.TMP_KPI06_SIRATPRICO_01
 ) WITH DATA NO PRIMARY INDEX;


/******************************* MEPECO SIRAT *********************************/
/******************************* MEPECO SIRAT *********************************/
/******************************* MEPECO SIRAT *********************************/

  -- Pago DDJJ 0616

 DROP TABLE BDDWESTG.TMP_KPI06_T03_0;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_T03_0
 AS(

  SELECT DISTINCT t03lltt_ruc numruc, t03periodo perpag, t03nabono nabono, t03formulario formul, t03norden norden 
  FROM BDDWESTG.T03DJCAB_DEPEN 
  WHERE t03periodo IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
  AND t03formulario IN ('0616','0116') 
  AND t03rechazado = '0'
  AND t03f_presenta <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD')

 ) WITH DATA NO PRIMARY INDEX ;


 DROP TABLE BDDWESTG.TMP_KPI06_T03;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_T03
 AS(

     SELECT DISTINCT numruc, perpag, CAST(formul AS SMALLINT) formul, norden
     FROM BDDWESTG.T04DJDET_DEPEN, BDDWESTG.TMP_KPI06_T03_0
     WHERE t04nabono = nabono
     AND t04formulario = formul
     AND t04norden = norden
     AND t04casilla = '355'
     AND t04i_valor IS NOT NULL
     AND t04i_valor*1 > 0
   
 ) WITH DATA NO PRIMARY INDEX ;

-- Pago Boletas 
 --t_origen_10

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_10;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_10
 AS(

   SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
   FROM BDDWESTG.CRT
   WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
   AND crt_codtri = '030401' 
   AND crt_formul NOT IN (116,616,1083,1683)
   AND crt_indaju = '0'
   AND crt_indpag IN (1,5)
   AND crt_estado <> '02'
   AND crt_imptri > 0
   AND crt_fecpag <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD')
     
 ) WITH DATA NO PRIMARY INDEX ;


-- Pago Boletas � No se considera boletas en proceso de compensaci�n:

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_1651;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_1651 as    --t_1651
 (
  SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, 
      CAST(b.num_doc_ori AS INTEGER) numdoc  
  from BDDWESTG.TMP_KPI06_SIRATMEPECO_10 a, BDDWESTG.t1651sol_comp b
  WHERE a.numruc=b.num_ruc  
  AND b.ind_con_com IN ('3','4','5')
  AND b.cod_eta_sol IN ('01','02','03')
  AND b.cod_tri ='030401'
  
 ) WITH DATA NO PRIMARY INDEX;


 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_09;
 --t_origen_09
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_09 as    
 (

  SELECT a.numruc, a.perpag, a.formul, a.numdoc 
  FROM BDDWESTG.TMP_KPI06_SIRATMEPECO_10 a
  LEFT JOIN BDDWESTG.TMP_KPI06_SIRATMEPECO_1651 b
  ON a.numruc=b.numruc  and a.formul=b.formul and a.numdoc= b.numdoc
  WHERE b.numruc  IS NULL
  AND b.formul IS NULL
  AND b.numdoc IS NULL
  
 ) WITH DATA NO PRIMARY INDEX;


-- Pago Boletas � No se considera boletas en proceso de devoluci�n:

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_tdev;
 --t_dev
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_tdev as    
 (
  SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_aso formul, b.num_doc_aso numdoc
  from BDDWESTG.TMP_KPI06_SIRATMEPECO_09 a, BDDWESTG.devoluciones b
  WHERE a.numruc=b.num_ruc  
  AND a.formul = b.cod_for_aso
  AND a.numdoc = b.num_doc_aso 
  AND b.cod_tip_sol = '02'
  AND b.ind_est_dev IN ('0','3')
  AND b.ind_res_dev IN ('0','F')
 ) WITH DATA NO PRIMARY INDEX;


 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_08;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_08 as    
 (   
 
  SELECT a.numruc, a.perpag, a.formul, a.numdoc 
  FROM BDDWESTG.TMP_KPI06_SIRATMEPECO_09 a
  LEFT JOIN BDDWESTG.TMP_KPI06_SIRATMEPECO_tdev b
  ON a.numruc=b.numruc  and a.formul=b.formul  and a.numdoc= b.numdoc
  WHERE b.numruc IS NULL
  AND b.formul IS NULL
  AND b.numdoc IS NULL
  
 ) WITH DATA NO PRIMARY INDEX;


-- Pago Boletas � No se considera boletas en proceso de reimputaci�n:

 --t_869

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_t869;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_t869 as    
 (    

  SELECT b.num_ruc numruc, b.cod_for formul, b.num_doc norden 
  FROM BDDWESTG.TMP_KPI06_SIRATMEPECO_08 a, BDDWESTG.t869rei_cab b
  WHERE a.numruc = b.num_ruc
  AND a.formul = b.cod_for
  AND a.numdoc = b.num_doc 
  AND b.cod_for_rei = '4715'
  AND b.ind_aplica = '0'
  AND b.ind_motivo NOT IN ('0','9')
 
 ) WITH DATA NO PRIMARY INDEX;


    INSERT INTO BDDWESTG.TMP_KPI06_T03
 SELECT a.numruc, a.perpag ,a.formul, a.numdoc
    FROM BDDWESTG.TMP_KPI06_SIRATMEPECO_08 a 
 LEFT JOIN BDDWESTG.TMP_KPI06_SIRATMEPECO_t869 b 
   ON a.numruc = b.numruc 
   AND a.formul = b.formul
   AND a.numdoc = b.norden
 WHERE b.numruc  IS NULL
 AND b.formul IS NULL
 AND b.norden IS NULL;

-- Bloque 03

 INSERT INTO BDDWESTG.TMP_KPI06_T03
 SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
 FROM BDDWESTG.CRT
 WHERE crt_perpag IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212')
 AND crt_codtri = '030401' 
 AND crt_indaju = '1'
 AND crt_tiptra = '1472'
 AND crt_imptri > 0
 AND crt_fecpag <= CAST('2023-02-24' AS DATE FORMAT 'YYYY-MM-DD');


-- Bloque 04
 --t_origen_06

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_06;
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_06 as    
 (    

  SELECT a.num_ruc_deu numruc, b.num_esc_exp numdoc
  FROM BDDWESTG.t3386doc_deu_com a, BDDWESTG.cab_pre_res b
  WHERE a.cod_tri_deu = '030401'
  AND a.num_pre_res = b.num_pre_res
  AND b.ind_est_pre = '1'
  AND b.ind_eta_pre = '2'
  AND a.ind_tip_deu = '01'
  AND a.cod_tip_cal IN ('023001', '023002')

 ) WITH DATA NO PRIMARY INDEX;


 INSERT INTO BDDWESTG.TMP_KPI06_T03
 SELECT  a.num_ruc numruc, a.per_tri_des perpag,a.cod_for,a.nro_orden
 FROM BDDWESTG.t1651sol_comp a, BDDWESTG.TMP_KPI06_SIRATMEPECO_06 b
 WHERE a.cod_for = '1648'
 AND a.nro_orden = b.numdoc 
 AND a.num_ruc = b.numruc 
 AND a.per_tri_des IN ('202201','202202','202203','202204','202205','202206','202207','202208','202209','202210','202211','202212');

 ----  RESUMEN SIRATMEPECO

 DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO;
 --t_origen_05
 CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO as    
 ( 
  SELECT DISTINCT numruc,perpag,formul ,norden
   FROM BDDWESTG.TMP_KPI06_T03  
 ) WITH DATA NO PRIMARY INDEX;

/***********************************************************************************************************************/ 
/***********************************************************************************************************************/
-------------------------PAGOS DIRECTOS EN TRANSACCIONAL PRICO Y MEPECO--------------------------------------------------


 DROP TABLE BDDWESTG.tmp093168_kpigr06_detcntpertr;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr06_detcntpertr  
 AS(
        SELECT
         x0.numruc,x0.perpag as periodo,x0.formul,x0.norden,
         coalesce(x1.ind_presdj,0) as ind_presdj
  FROM(
            SELECT a.numruc, a.perpag,a.formul,a.norden 
      FROM BDDWESTG.TMP_KPI06_SIRATPRICO a , BDDWESTG.DDP_DEPEN b 
      WHERE a.numruc=b.ddp_numruc 
      UNION 
      SELECT a.numruc,a.perpag,a.formul ,a.norden 
      FROM BDDWESTG.TMP_KPI06_SIRATMEPECO a , BDDWESTG.DDP_DEPEN b 
      WHERE a.numruc=b.ddp_numruc
          ) x0
          INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.numruc = x1.num_ruc  
 ) WITH DATA NO PRIMARY INDEX ;


--------------------------PAGOS DIRECTOS  EN  FVIRTUAL------------------------------------------------------


 DROP TABLE BDDWESTG.tmp093168_kpigr06_detcntperfv;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr06_detcntperfv
 AS( 
 SELECT DISTINCT x1.num_ruc,
     SUBSTR(x0.periodo,3,4)||SUBSTR(x0.periodo,1,2) as periodo,
     CAST(x0.num_formul AS smallint) as cod_formul,
     CAST(x0.num_ordope AS BIGINT) as num_ordope,
     coalesce(x1.ind_presdj,0) as ind_presdj
 FROM BDDWESTG.T5409CAS127 x0
 INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
 ) WITH DATA NO PRIMARY INDEX;


-------------------------PAGOS DIRECTOS  EN  MONGOBB------------------------------------------------------

 
 DROP TABLE BDDWESTG.tmp093168_kpigr06_detcntpermdb;
 CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr06_detcntpermdb
    AS( 

 SELECT 
    x1.num_ruc,
       substr(num_perpago,3,4)||substr(num_perpago,1,2) as periodo,
       cast(x0.cod_formul as smallint) as cod_formul,
       x0.num_numorden as num_ordope,
       coalesce(x1.ind_presdj,0) as ind_presdj
 FROM BDDWESTG.T5409CAS127_MONGODB x0
 INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
 ) WITH DATA NO PRIMARY INDEX;


/**************************************************************************************/
/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTG.tmp093168_kpigr06_cnorigen;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr06_cnorigen AS
(
  SELECT ind_presdj,count(periodo) as cant_per_origen
  FROM BDDWESTG.tmp093168_kpigr06_detcntpertr
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual

DROP TABLE BDDWESTG.tmp093168_kpigr06_cndestino1;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr06_cndestino1 AS
(
  SELECT ind_presdj,count(periodo) as cant_per_destino1
  FROM BDDWESTG.tmp093168_kpigr06_detcntperfv
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB


DROP TABLE BDDWESTG.tmp093168_kpigr06_cndestino2;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr06_cndestino2 AS
(
  SELECT ind_presdj,count(periodo) as cant_per_destino2
  FROM BDDWESTG.tmp093168_kpigr06_detcntpermdb
  GROUP BY 1
) WITH DATA NO PRIMARY INDEX;


/************************ INSERTA CONTEOS A TABLAS DE DETALLE **************************/

 DELETE FROM BDDWESTG.T11908DETKPITRIBINT  
  WHERE COD_KPI='K006012022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
         'K006012022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
             case when x0.ind_presdj=0 then (select coalesce(sum(cant_per_origen),0) from BDDWESTG.tmp093168_kpigr06_cnorigen) else 0 end as cant_origen,
             coalesce(x1.cant_per_destino1,0) as cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
          from
          (
            select * from BDDWESTG.tmp093168_kpigr06_cnorigen
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr06_cndestino1 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;


  DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
  WHERE COD_KPI='K006022022' AND FEC_CARGA=CURRENT_DATE;

  INSERT INTO BDDWESTG.T11908DETKPITRIBINT  
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
          'K006022022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             case when x0.ind_presdj=0  then (select coalesce(sum(cant_per_destino2),0) from BDDWESTG.tmp093168_kpigr06_cndestino2) else 0 end AS cant_destino
      FROM 
      (
          select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
          from
          (
            select * from BDDWESTG.tmp093168_kpigr06_cndestino1
            union all select 1,0 from (select '1' agr1) a
            union all select 0,0 from (select '0' agr0) b
          ) y group by 1
      ) x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr06_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;

 
/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/ 

DROP TABLE BDDWESTG.tmp093168_dif_K006012022 ;
CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K006012022 AS (
     SELECT DISTINCT 
  y0.numruc as num_ruc_trab,y0.periodo,y0.formul,y0.norden
 FROM (
  SELECT
            numruc,periodo,formul,norden  
  FROM BDDWESTG.tmp093168_kpigr06_detcntpertr
  EXCEPT ALL
  SELECT 
           num_ruc,periodo,cod_formul,num_ordope
        FROM BDDWESTG.tmp093168_kpigr06_detcntperfv
 ) y0
 ) WITH DATA NO PRIMARY INDEX;


DROP TABLE BDDWESTG.tmp093168_dif_K006022022 ;
CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K006022022 AS (
 SELECT DISTINCT 
  y0.num_ruc as num_ruc_trab,y0.periodo,y0.cod_formul,y0.num_ordope 
 FROM (
     SELECT 
           num_ruc,periodo,cod_formul,num_ordope
        FROM BDDWESTG.tmp093168_kpigr06_detcntperfv
  EXCEPT ALL
  SELECT 
           num_ruc,periodo,cod_formul,num_ordope
        FROM BDDWESTG.tmp093168_kpigr06_detcntpermdb
 ) y0
    ) WITH DATA NO PRIMARY INDEX;

    
/*********************************************************************/

 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K006012022 
 ORDER BY num_ruc_trab,periodo;

 LOCK ROW FOR ACCESS
 SELECT * FROM BDDWESTG.tmp093168_dif_K006022022
 ORDER BY num_ruc_trab,periodo;

/************************ BORRA TABLAS TEMPORALES **********************/
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_10;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_1651;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_09;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_tdevo;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_08;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_t869;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_01;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_06;
--DROP TABLE BDDWESTG.TMP_KPI06_T03_0;
--DROP TABLE BDDWESTG.TMP_KPI06_T03;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_10;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_1651;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_09;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_tdev;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_08;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_t869;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_06;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO;
--DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO;
--DROP TABLE BDDWESTG.tmp093168_kpigr06_detcntpertr;
--DROP TABLE BDDWESTG.tmp093168_kpigr06_detcntperfv;
--DROP TABLE BDDWESTG.tmp093168_kpigr06_detcntpermdb;
--DROP TABLE BDDWESTG.tmp093168_kpigr06_cnorigen;
--DROP TABLE BDDWESTG.tmp093168_kpigr06_cndestino1;
--DROP TABLE BDDWESTG.tmp093168_kpigr06_cndestino2;

/************************ FIN BORRA TABLAS TEMPORALES **********************/

/*
DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1651;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_06;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_tdev;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_treimp;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_05;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_01;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_1651;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_06;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_tdev;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_treimp;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_02;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_05;
DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO;
DROP TABLE BDDWESTG.TMP_KPI07_cuentaplame;
DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_ND;
DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_D;
DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_D_ND;
DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_relacion;
DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB;
DROP TABLE BDDWESTG.TMP_KPI07_DIF_RECNOFVIR;
DROP TABLE BDDWESTG.TMP_KPI07_DIF_FVIRNOMODB;
*/



/* ---- INICIO PASO1 EXTRAE PAGOS SIRAT */

/******************************* PRICO *********************************/


  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1;
  --t_origen_01
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1 as    
  (
   SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, 
   --SUBSTR(crt_ndocpa,1,10) numdoc substr indicado en documento de cuadratura v18
   CAST(crt_ndocpa as varchar(25))  AS numdoc    -- adaptacion segun CR1928 31012022
     FROM BDDWESTG.CRT
   WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
     AND crt_codtri = '030501'
     AND crt_indaju = '0'
     AND crt_indpag IN (1,5)
     AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
     AND crt_estado <> '02'
   UNION
   SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, 
   --SUBSTR(crt_ndocpa,1,10) numdoc
   CAST(crt_ndocpa as varchar(25))  AS numdoc
     FROM BDDWESTG.CRT
   WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
     AND crt_codtri = '030501'                                      
     AND crt_tiptra = '2962'
     AND crt_indaju = '1'
     AND crt_indpag IN (1,5)
     AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
     AND crt_estado <> '02'                
  ) WITH DATA NO PRIMARY INDEX;

 
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_2;
 
  --t_origen_02
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_2 as    
  (
    SELECT crt_numruc numruc, crt_perpag perpag, 1648 formul, crt_docori numdoc 
     FROM BDDWESTG.CRT
    WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
      AND crt_codtri = '030501'
      AND crt_tiptra = '1472'
      AND crt_indaju = '1'
      AND crt_imptri > 0
      AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
  ) WITH DATA NO PRIMARY INDEX;


   INSERT INTO BDDWESTG.TMP_KPI07_SIRATPRICO_1
   SELECT numruc, perpag, formul, numdoc  
    FROM BDDWESTG.TMP_KPI07_SIRATPRICO_2 a, BDDWESTG.cab_pre_res b
   WHERE b.num_res = a.numdoc
    AND b.cod_tip_doc = '023000'
    AND b.ind_est_pre = '1'
    AND b.ind_eta_pre = '2';


-- Exclusiones Pago en Proceso de compensaci�n



  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_1651;
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

 
--compensaciones


  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_06;
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


  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_tdev;

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

 
  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_treimp;
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


  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO_05;

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
  SELECT numruc, per_tri_des, 1, 1
  FROM BDDWESTG.t1651sol_comp,BDDWESTG.TMP_KPI07_SIRATPRICO_05
  WHERE cod_for = '1648'
    AND nro_orden = numdoc 
    AND num_ruc = numruc 
    AND per_tri_des IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212);


  --DROP TABLE BDDWESTG.TMP_KPI07_SIRATPRICO;
   --t_origen_03
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATPRICO as    
  ( 

   SELECT numruc, perpag
    FROM BDDWESTG.TMP_KPI07_SIRATPRICO_06
   GROUP BY 1, 2 
  ) WITH DATA NO PRIMARY INDEX;

 
  
  --- RESULTADO : TABLA BDDWESTG.TMP_KPI07_SIRATPRICO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT PRICO   	
  ---             Campos : NUMRUC , PERPAG

 /******************************* MEPECO SIRAT *********************************/
  -- PAGOS DDJJ 0616


     

     --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_01;
      --t_origen_01
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_01 
     AS(

       SELECT crt_numruc AS numruc, crt_perpag AS perpag, crt_formul AS formul, 
       CAST(crt_ndocpa as varchar(25))  AS numdoc
        FROM BDDWESTG.CRT
       WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
         AND crt_codtri = '030501'
         AND crt_formul NOT IN (1083,1683,116,616)
         AND crt_indaju = '0'
         AND crt_indpag IN (1,5)
         AND crt_estado <> '02'
         AND crt_imptri > 0
         AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
       GROUP BY 1,2,3,4

     ) WITH DATA NO PRIMARY INDEX ;

 
  -- Exclusiones pago en proceso de compensacion 			

     --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_1651;
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


 
     --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_06;
 
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

 
   --Exclusiones Pago en Proceso de devoluci�n

   
     
     --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_tdev;

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


   --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_treimp;
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



     --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_02;

     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_02 as    --t_origen_02
     (  
      SELECT crt_numruc AS numruc, crt_perpag AS perpag, 1648 AS formul, crt_docori AS numdoc
      FROM BDDWESTG.CRT
      WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
        AND crt_codtri = '030501'
        AND crt_tiptra = '1472'
        AND crt_indaju = '1'
        AND crt_imptri > 0
        AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
     ) WITH DATA NO PRIMARY INDEX;

 

     INSERT INTO  BDDWESTG.TMP_KPI07_SIRATMEPECO_01
     SELECT numruc, perpag, formul, numdoc  
      FROM BDDWESTG.TMP_KPI07_SIRATMEPECO_02 a, BDDWESTG.cab_pre_res b
     WHERE b.num_res = a.numdoc
      AND b.cod_tip_doc = '023000'
      AND b.ind_est_pre = '1'
      AND b.ind_eta_pre = '2';

 

  -- COMPENSACIONES A VALORES (CRT)

 
      --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO_05;
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
     SELECT numruc, per_tri_des, 1, 1
     FROM BDDWESTG.t1651sol_comp, BDDWESTG.TMP_KPI07_SIRATMEPECO_05
     WHERE cod_for = '1648'
     AND nro_orden = numdoc 
     AND num_ruc = numruc 
     AND per_tri_des IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212);

 
----  RESUMEN SIRATMEPECO

     
     --DROP TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_SIRATMEPECO as    --t_origen_03
     ( 

      SELECT numruc, perpag
       FROM BDDWESTG.TMP_KPI07_SIRATMEPECO_01
      GROUP BY 1, 2 
     ) WITH DATA NO PRIMARY INDEX;

     
      
   --- RESULTADO : TABLA BDDWESTG.TMP_KPI07_SIRATMEPECO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT MEPECO
   ---             Campos : NUMRUC , PERPAG


/*FIN    PASO1 ---------------------------------------------------------------------/
/**************************FIN MEPECO SIRAT ****************************************/


/************************* INICIO TABLAS ORIGEN FVIRTUAL ***************************** /
		/*INICIO PASO1 ---------------------------------------------------------------------*/

     -- Aun no Declarado (ND) -- t_rucs_fvirtual_01
    /*
     --DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_ND;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_selecfvirtual_ND
     AS(
      
        SELECT num_ruc,MAX(num_sec) num_sec FROM BDDWESTG.t5847ctldecl 
        WHERE num_ejercicio = 2022
        AND num_formul = '0709' 
        AND ind_actual = '1' 
        AND ind_estado = '0' 
        AND ind_proceso = '1'
        AND cast(fec_creacion as date) <= CAST('2023-02-08' AS DATE FORMAT 'YYYY-MM-DD')
        GROUP BY 1
                
                                
     ) WITH DATA NO PRIMARY INDEX ;

     -- Declarado   (D) -- t_rucs_fvirtual_02


     --DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_D;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_selecfvirtual_D
     AS(     

        SELECT num_ruc,MAX(num_sec) num_sec FROM BDDWESTG.t5847ctldecl 
        WHERE num_ejercicio = 2022
        AND num_formul = '0709' 
        AND ind_estado = '2'
         AND cast(fec_creacion as date) <= CAST('2023-02-08' AS DATE FORMAT 'YYYY-MM-DD')
        GROUP BY 1        
        
     ) WITH DATA NO PRIMARY INDEX ;


     -- Cruza Declarado y No Declarado
     
     --DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_D_ND;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_selecfvirtual_D_ND
     AS(
     
        SELECT num_ruc, num_sec FROM BDDWESTG.TMP_KPI07_selecfvirtual_ND 
        WHERE num_ruc NOT IN (SELECT num_ruc FROM BDDWESTG.TMP_KPI07_selecfvirtual_D)
        
        
     ) WITH DATA NO PRIMARY INDEX ;
*/
  
  /*FIN    PASO1 ---------------------------------------------------------------------*/
  /*INICIO PASO2 ---------------------------------------------------------------------*/

     -- a.	Cuando el contribuyente a�n no present� su DDJJ Anual (crea tabla):

    --DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_relacion;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_selecfvirtual_relacion
     AS(

      SELECT DISTINCT b.num_ruc,'ND' as ind_declara,
      SUBSTR(A.PERIODO,3,4)||SUBSTR(A.PERIODO,1,2) PERIODO
      FROM BDDWESTG.T5410CAS128 a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=0)  b 
      ON a.num_sec = b.num_sec
      --WHERE b.num_formul = '0709' 
      --AND b.num_ejercicio = 2022
     
     ) WITH DATA NO PRIMARY INDEX ;


     -- b.	Cuando el contribuyente ya present� su DDJJ Anual (inserta data):
     
      INSERT INTO BDDWESTG.TMP_KPI07_selecfvirtual_relacion
      SELECT DISTINCT b.num_ruc,'D' as ind_declara,
      SUBSTR(A.PERIODO,3,4)||SUBSTR(A.PERIODO,1,2) PERIODO
      FROM BDDWESTG.T5410CAS128 a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=1) b 
      ON a.num_sec = b.num_sec;

     
     --- RESULTADO : Tabla BDDWESTG.TMP_KPI07_selecfvirtual_relacion que contiene el detalle
     --- 			de los rucs que presentaron y no presentaron declaracion
     ---             ruc declarante (num_ruc),doc declarado (num_doc),D/ND declara o no declara (ind_declara),ejercicio
     ---				
  /*FIN    PASO2 ---------------------------------------------------------------------*/

/************************* FIN TABLAS FVIRTUAL *****************************/
/************************* INICIO TABLAS MONGODB ***************************** /

		--- OBSERVACION : Para la seleccion de casos que no estan se usa la misma temporal de casos de fvirtual
		--- Segun indicaci�n es la misma logica de seleccion
		

		/**INICIO PASO1 ---------------------------------------------------------------------**/

     -- a.	Cuando el contribuyente a�n no present� su DDJJ Anual (crea tabla):

     --DROP TABLE BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB
     AS(

      SELECT DISTINCT b.num_ruc,'ND' as ind_declara
      ,SUBSTR(A.NUM_PERPAGO,3,4)||SUBSTR(A.NUM_PERPAGO,1,2) PERIODO
      FROM BDDWESTG.T5410CAS128_mongodb a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=0)  b ON a.num_sec = b.num_sec
      --WHERE b.num_formul = '0709' 
      --AND b.num_ejercicio = 2022
            
     ) WITH DATA NO PRIMARY INDEX ;


     -- b.	Cuando el contribuyente ya present� su DDJJ Anual (inserta data):
     
      INSERT INTO BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB
      SELECT DISTINCT b.num_ruc,'D' as ind_declara
      ,SUBSTR(A.NUM_PERPAGO,3,4)||SUBSTR(A.NUM_PERPAGO,1,2) PERIODO
      FROM BDDWESTG.T5410CAS128_mongodb a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=1)  b ON a.num_sec = b.num_sec;

     --- RESULTADO : Tabla BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB que contiene el detalle
     --- 			de los rucs que presentaron y no presentaron declaracion
     ---             ruc empleador,doc empleado,D/ND declara o no declara,periodo en MONGODB
  /**FIN    PASO1 ---------------------------------------------------------------------**/

/************************* FIN TABLAS MONGODB *****************************/

----

/************************ GENERACION DE DIFERENCIAS **************************/
   
   /**-- REGISTROS QUE ESTAN EN RECAUDA y NO EN FVIRTUAL*/

       --DROP TABLE BDDWESTG.TMP_KPI07_cuentaplame;


      CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_cuentaplame
      AS(
        
        SELECT tr.numruc,tr.perpag
        FROM
       (
      SELECT a.numruc, a.perpag FROM BDDWESTG.TMP_KPI07_SIRATPRICO a , BDDWESTG.DDP_DEPEN b WHERE a.numruc=b.ddp_numruc
       UNION
       SELECT a.numruc, a.perpag FROM BDDWESTG.TMP_KPI07_SIRATMEPECO a , BDDWESTG.DDP_DEPEN b WHERE a.numruc=b.ddp_numruc      
       ) tr
      where SUBSTR(tr.numruc,1,1)<>'2' OR tr.numruc IN (SELECT num_ruc FROM bddwestg.tmp093168_rucs20_incluir)
      ) WITH DATA NO PRIMARY INDEX ;


       --DROP TABLE BDDWESTG.TMP_KPI07_DIF_RECNOFVIR;

      CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_DIF_RECNOFVIR
      AS(
    
       SELECT distinct numruc, perpag FROM BDDWESTG.TMP_KPI07_cuentaplame
       --WHERE a.numruc NOT IN ( SELECT DISTINCT num_ruc FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion)
       EXCEPT ALL
       SELECT DISTINCT num_ruc, PERIODO FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion
       
      ) WITH DATA NO PRIMARY INDEX ;

   
   /** REGISTROS QUE ESTAN EN FVIRTUAL Y NO EN MONGODB*/


       --DROP TABLE BDDWESTG.TMP_KPI07_DIF_FVIRNOMODB;
      CREATE MULTISET TABLE BDDWESTG.TMP_KPI07_DIF_FVIRNOMODB
      AS(

      SELECT DISTINCT num_ruc, PERIODO FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion 
      --WHERE a.num_ruc NOT IN ( SELECT DISTINCT num_ruc FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB)
      EXCEPT ALL
       SELECT DISTINCT num_ruc, PERIODO FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB
      ) WITH DATA NO PRIMARY INDEX ;


/************************ FIN GENERACION DE DIFERENCIAS  **********************/

/************************ GENERACION DE ARCHIVOS **************************/

  -- EN RECAUDA PERO NO EN FVIRTUAL
 -- .EXPORT FILE /work1/teradata/dat/093168/DIF_K007012022_CAS127_TRANVSFVIR_20230206.unl;

   LOCK ROW FOR ACCESS
      SELECT * FROM BDDWESTG.TMP_KPI07_DIF_RECNOFVIR ORDER BY 1,2;

  -- EN FVIRTUAL PERO NO EN MONGODB
  --.EXPORT FILE /work1/teradata/dat/093168/DIF_K007022022_CAS127_FVIRVSMODB_20230206.unl;
 
   LOCK ROW FOR ACCESS
      SELECT * FROM BDDWESTG.TMP_KPI07_DIF_FVIRNOMODB ORDER BY 1,2;

 /************************ FIN GENERACION DE ARCHIVOS **************************/

/************************ INSERTA CONTEOS A TABLAS DE DETALLE **************************/

  -- recauda vs fvirtual	VS MONGODB
  -- BDDWESTG.TMP_KPI07_cuentaplamenum_rucs,num_ruc,per_decla  
  -- BDDWESTG.TMP_KPI07_selecfvirtual_relacion   ruc declarante (num_ruc),doc declarado (num_doc),D/ND declara o no declara (ind_declara),periodo 

     -- BORRA REGISTROS CARGADOS EN LA MISMA FECHA
                    
                    DELETE FROM BDDWESTG.T11908DETKPITRIBINT WHERE COD_KPI IN ('K007012022','K007022022') AND FEC_CARGA=CURRENT_DATE;

 
     -- INSERTA CONTEO DECLARANTES RECAUDA Y FVIRTUAL
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','1',
       'K007012022',
       0,              
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion WHERE IND_DECLARA='D'),
       CURRENT_DATE);

 


     -- INSERTA CONTEO NO DECLARANTES RECAUDA Y FVIRTUAL
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','0',
       'K007012022',
       (SELECT COUNT(numruc) FROM BDDWESTG.TMP_KPI07_cuentaplame),                     
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion WHERE IND_DECLARA='ND'),
       CURRENT_DATE);

     -- INSERTA CONTEO DECLARANTES FVIRTUAL Y MONGODB
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','1',
       'K007022022',
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion WHERE IND_DECLARA='D'),
       0,
       CURRENT_DATE);

     -- INSERTA CONTEO DECLARANTES FVIRTUAL Y MONGODB
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','0',
       'K007022022',
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion WHERE IND_DECLARA='ND'),
       (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI07_selecfvirtual_relacion_MDB),
       CURRENT_DATE);


/*********************** FIN INSERTA CONTEOS A TABLAS DE DETALLE **********************/
/************************ BORRA TABLAS TEMPORALES **********************/




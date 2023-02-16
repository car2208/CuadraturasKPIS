/*
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_10;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_1651;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_09;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_tdevo;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_08;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_t869;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_01;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_06;
DROP TABLE BDDWESTG.TMP_KPI06_T03_0;
DROP TABLE BDDWESTG.TMP_KPI06_T03;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_10;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_1651;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_09;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_tdev;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_08;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_t869;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_06;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO;
DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO;
DROP TABLE BDDWESTG.TMP_KPI06_cuentaplame;
DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_ND;
DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_D;
DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_D_ND;
DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_relacion;
DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_relacion_MDB;
DROP TABLE BDDWESTG.TMP_KPI06_DIF_RECNOFVIR;
DROP TABLE BDDWESTG.TMP_KPI06_DIF_FVIRNOMODB;
*/

  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_10;
  
  --t_origen_10
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_10 as   
  (
   
    SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
      FROM BDDWESTG.CRT
    WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
      AND crt_codtri = '030401'
      AND crt_indaju = '0'
      AND crt_indpag IN (1,5)
      AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
      AND crt_estado <> '02'
    UNION
    SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa norden
      FROM BDDWESTG.CRT
    WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
      AND crt_codtri = '030401'                                      
      AND crt_tiptra = '2962'
      AND crt_indaju = '1'
      AND crt_indpag IN (1,5)
      AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
      AND crt_estado <> '02'

  ) WITH DATA NO PRIMARY INDEX;




  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_1651;
  --t_1651
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



  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_09;
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


  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_tdevo;
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



  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_08;
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


  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_t869;
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

 



  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_01;
  --t_origen_01
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_01 as    
  (
  
   SELECT a.numruc, a.perpag 
   FROM BDDWESTG.TMP_KPI06_SIRATPRICO_08 a LEFT JOIN BDDWESTG.TMP_KPI06_SIRATPRICO_t869 b 
   ON a.numruc = b.numruc AND a.formul = b.formul AND a.norden = b.norden
   WHERE b.numruc  IS NULL
   AND b.formul IS NULL
   AND b.norden IS NULL
  
  ) WITH DATA NO PRIMARY INDEX;
  
  
  --Saldo a favor aplicado (hsf):
   
   INSERT INTO BDDWESTG.TMP_KPI06_SIRATPRICO_01
   SELECT hsf_numruc numruc, hsf_perpag perpag
   FROM BDDWESTG.HSF
   WHERE hsf_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
   AND hsf_codtri = '030401'
   AND hsf_tiptra = '1041'
   AND hsf_tipcta = '01'
   AND hsf_fecsaf <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD');



  --  Otros cr�ditos de ley (dbt/doc):

   INSERT INTO BDDWESTG.TMP_KPI06_SIRATPRICO_01
   SELECT a.dbt_numruc numruc, a.dbt_perpag perpag 
   FROM BDDWESTG.DBT a,BDDWESTG.DOC b
   WHERE a.dbt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
   AND dbt_codtri = '030401'
   AND dbt_tiptra = 1011
   AND dbt_indrec = 0
   AND dbt_fecdoc <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
   AND doc_formul = dbt_formul
   AND doc_numdoc = dbt_numdoc
   AND doc_codcas = 347
   AND doc_valdec > 0;

    
  -- Compensaciones
  
   INSERT INTO BDDWESTG.TMP_KPI06_SIRATPRICO_01
   SELECT dbt_numruc numruc, dbt_perpag perpag
   FROM BDDWESTG.DB2, BDDWESTG.DBT
   WHERE dbt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
   AND dbt_codtri = '030401'
   AND dbt_tiptra = 1011
   AND dbt_fecdoc <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
   AND dbt_formul = db2_formul
   AND dbt_numdoc = db2_numdoc
   AND dbt_codtri = db2_codtri
   AND db2_compen > 0
   UNION
   SELECT crt_numruc numruc, crt_perpag perpag
   FROM BDDWESTG.CRT
   WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
   AND crt_codtri = '030401'  
   AND crt_tiptra = '1272'
   AND crt_indaju = '1'
   AND crt_imptri > 0
   AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD');

  
  -- BLOQUE05


  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO_06;
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
   SELECT a.num_ruc numruc, per_tri_des perpag
   FROM BDDWESTG.t1651sol_comp a,BDDWESTG.TMP_KPI06_SIRATPRICO_06 b
   WHERE a.cod_for = '1648'
   AND a.nro_orden = b.numdoc 
   AND a.num_ruc = b.numruc 
   AND per_tri_des IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212);



  -- depura duplicados
  --DROP TABLE BDDWESTG.TMP_KPI06_SIRATPRICO;
  --t_origen_02
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATPRICO as    
  ( 

   SELECT numruc, perpag
    FROM BDDWESTG.TMP_KPI06_SIRATPRICO_01
   GROUP BY 1, 2 
  ) WITH DATA NO PRIMARY INDEX;

  --- RESULTADO : TABLA BDDWESTG.TMP_KPI06_SIRATPRICO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT PRICO   	
  ---             Campos : numruc , perpag

 /******************************* MEPECO SIRAT *********************************/
  -- Pago DDJJ 0616

     --t_origen_03



     --DROP TABLE BDDWESTG.TMP_KPI06_T03_0;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_T03_0
     AS(

        SELECT t03lltt_ruc numruc, t03periodo perpag, t03nabono nabono, t03formulario formul, t03norden norden 
        FROM BDDWESTG.T03DJCAB_DEPEN 
        WHERE t03periodo IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
        AND t03formulario IN ('0616','0116') 
        AND t03rechazado = '0'
        AND t03f_presenta <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')

     ) WITH DATA NO PRIMARY INDEX ;

 
     
     --t_origen_04


     --DROP TABLE BDDWESTG.TMP_KPI06_T03;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_T03
     AS(

       SELECT numruc, perpag 
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


     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_10;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_10
     AS(

       SELECT crt_numruc numruc, crt_perpag perpag, crt_formul formul, crt_ndocpa numdoc
       FROM BDDWESTG.CRT
       WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
       AND crt_codtri = '030401' 
       AND crt_formul NOT IN (116,616,1083,1683)
       AND crt_indaju = '0'
       AND crt_indpag IN (1,5)
       AND crt_estado <> '02'
       AND crt_imptri > 0
       AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD')
         
     ) WITH DATA NO PRIMARY INDEX ;


  
   -- Pago Boletas � No se considera boletas en proceso de compensaci�n:


     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_1651;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_1651 as    --t_1651
     (
      SELECT  b.num_ruc numruc, a.perpag perpag, b.cod_for_ori formul, b.num_doc_ori numdoc  
      from BDDWESTG.TMP_KPI06_SIRATMEPECO_10 a, BDDWESTG.t1651sol_comp b
      WHERE a.numruc=b.num_ruc  
      AND b.ind_con_com IN ('3','4','5')
      AND b.cod_eta_sol IN ('01','02','03')
      AND b.cod_tri ='030401'
      
     ) WITH DATA NO PRIMARY INDEX;



     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_09;
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


     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_tdev;    
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


     
     --t_origen_08


     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_08;
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

     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_t869;
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
     SELECT a.numruc, a.perpag FROM BDDWESTG.TMP_KPI06_SIRATMEPECO_08 a 
     LEFT JOIN BDDWESTG.TMP_KPI06_SIRATMEPECO_t869 b 
       ON a.numruc = b.numruc 
       AND a.formul = b.formul
       AND a.numdoc = b.norden
     WHERE b.numruc  IS NULL
     AND b.formul IS NULL
     AND b.norden IS NULL;

   -- Bloque 03
   
     INSERT INTO BDDWESTG.TMP_KPI06_T03
     SELECT crt_numruc numruc, crt_perpag perpag
     FROM BDDWESTG.CRT
     WHERE crt_perpag IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212)
     AND crt_codtri = '030401' 
     AND crt_indaju = '1'
     AND crt_tiptra = '1472'
     AND crt_imptri > 0
     AND crt_fecpag <= CAST('2023-02-06' AS DATE FORMAT 'YYYY-MM-DD');
   
   -- Bloque 04
     --t_origen_06
     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO_06;
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
     SELECT numruc, per_tri_des
     FROM BDDWESTG.t1651sol_comp, BDDWESTG.TMP_KPI06_SIRATMEPECO_06
     WHERE cod_for = '1648'
     AND nro_orden = numdoc 
     AND num_ruc = numruc 
     AND per_tri_des IN (202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212);

     ----  RESUMEN SIRATMEPECO

     --DROP TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO;
     --t_origen_05
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_SIRATMEPECO as    
     ( 
      SELECT numruc, perpag
       FROM BDDWESTG.TMP_KPI06_T03
      GROUP BY 1, 2 
      
     ) WITH DATA NO PRIMARY INDEX;

   --- RESULTADO : TABLA BDDWESTG.TMP_KPI06_SIRATMEPECO QUE CONTIENE LA RELACION DE PRESENTACIONES DE SIRAT MEPECO
   ---             Campos : numruc , perpag


/*FIN    PASO1 ---------------------------------------------------------------------/
/*INICIO PASO2 ---------------------------------------------------------------------/

/************************* INICIO TABLAS ORIGEN FVIRTUAL ***************************** /

		/*INICIO PASO1 ---------------------------------------------------------------------*/
/*
     -- Aun no Declarado (ND)
     -- t_rucs_fvirtual_01
     SELECT 1 FROM dbc.TablesV WHERE databasename = 'BDDWESTG' AND TableName = 'TMP_KPI06_selecfvirtual_ND';

     --DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_ND;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_selecfvirtual_ND
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


     -- Declarado   (D)
     -- t_rucs_fvirtual_02

     --DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_D;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_selecfvirtual_D
     AS(     

        SELECT num_ruc,MAX(num_sec) num_sec FROM BDDWESTG.t5847ctldecl 
        WHERE num_ejercicio = 2022
        AND num_formul = '0709' 
        AND ind_estado = '2'
         AND cast(fec_creacion as date) <= CAST('2023-02-08' AS DATE FORMAT 'YYYY-MM-DD')
        GROUP BY 1        
        
     ) WITH DATA NO PRIMARY INDEX ;


     --DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_D_ND;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_selecfvirtual_D_ND
     AS(
     
        SELECT num_ruc, num_sec FROM BDDWESTG.TMP_KPI06_selecfvirtual_ND 
        WHERE num_ruc NOT IN (SELECT num_ruc FROM BDDWESTG.TMP_KPI06_selecfvirtual_D)
        
        
     ) WITH DATA NO PRIMARY INDEX ;
*/
 
  /*FIN    PASO1 ---------------------------------------------------------------------*/
  /*INICIO PASO2 ---------------------------------------------------------------------*/

     -- a.	Cuando el contribuyente a�n no present� su DDJJ Anual (crea tabla):

     --DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_relacion;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_selecfvirtual_relacion
     AS(

      SELECT b.num_ruc,'ND' as ind_declara,
      SUBSTR(A.PERIODO,3,4)||SUBSTR(A.PERIODO,1,2) PERIODO
      FROM BDDWESTG.T5409CAS127 a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=0)  
      b ON a.num_sec = b.num_sec
      --WHERE b.num_formul = '0709' 
      --AND b.num_ejercicio = 2022
     
     ) WITH DATA NO PRIMARY INDEX ;


     -- b.	Cuando el contribuyente ya present� su DDJJ Anual (inserta data):
     
      INSERT INTO BDDWESTG.TMP_KPI06_selecfvirtual_relacion
      SELECT distinct b.num_ruc,'D' as ind_declara,
      SUBSTR(A.PERIODO,3,4)||SUBSTR(A.PERIODO,1,2) PERIODO
      FROM BDDWESTG.T5409CAS127 a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=1) 
      b ON a.num_sec = b.num_sec;

     
     --- RESULTADO : Tabla BDDWESTG.TMP_KPI06_selecfvirtual_relacion que contiene el detalle
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


     --DROP TABLE BDDWESTG.TMP_KPI06_selecfvirtual_relacion_MDB;
     CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_selecfvirtual_relacion_MDB
     AS(

      SELECT b.num_ruc,'ND' as ind_declara
      ,SUBSTR(A.NUM_PERPAGO,3,4)||SUBSTR(A.NUM_PERPAGO,1,2) PERIODO
      FROM BDDWESTG.T5409CAS127_MONGODB a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=0)  b 
      ON a.num_sec = b.num_sec
            
     ) WITH DATA NO PRIMARY INDEX ;


     -- b.	Cuando el contribuyente ya present� su DDJJ Anual (inserta data):
     
      INSERT INTO BDDWESTG.TMP_KPI06_selecfvirtual_relacion_MDB
      SELECT distinct b.num_ruc,'D' as ind_declara
      ,SUBSTR(A.NUM_PERPAGO,3,4)||SUBSTR(A.NUM_PERPAGO,1,2) PERIODO
      FROM BDDWESTG.T5409CAS127_MONGODB a 
      INNER JOIN (SELECT num_ruc FROM BDDWESTG.tmp093168_kpiperindj WHERE ind_presdj=1) b 
      ON a.num_sec = b.num_sec;

     
     --- RESULTADO : Tabla BDDWESTG.TMP_KPI09_selecfvirtual_relacion_MDB que contiene el detalle
     --- 			de los rucs que presentaron y no presentaron declaracion
     ---             ruc empleador,doc empleado,D/ND declara o no declara,periodo en MONGODB
  /**FIN    PASO1 ---------------------------------------------------------------------**/

/************************* FIN TABLAS MONGODB *****************************/

----

/************************ GENERACION DE DIFERENCIAS **************************/
   
   /***-- REGISTROS QUE ESTAN EN RECAUDA y NO EN FVIRTUAL*/

       --DROP TABLE BDDWESTG.TMP_KPI06_cuentaplame;
      CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_cuentaplame  
      AS(
        select  tr.numruc,tr.perpag
        from 
        (
        SELECT a.numruc, a.perpag FROM BDDWESTG.TMP_KPI06_SIRATPRICO a , BDDWESTG.DDP_DEPEN b WHERE a.numruc=b.ddp_numruc 
        UNION 
        SELECT a.numruc, a.perpag FROM BDDWESTG.TMP_KPI06_SIRATMEPECO a , BDDWESTG.DDP_DEPEN b WHERE a.numruc=b.ddp_numruc 
        ) tr
        where SUBSTR(tr.numruc,1,1)<>'2' OR tr.numruc IN (SELECT num_ruc FROM bddwestg.tmp093168_rucs20_incluir)
       
      ) WITH DATA NO PRIMARY INDEX ;


  --DROP TABLE BDDWESTG.TMP_KPI06_DIF_RECNOFVIR;
  CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_DIF_RECNOFVIR
      AS(
    
       SELECT distinct a.numruc AS num_ruc_trab, perpag as per_dif FROM BDDWESTG.TMP_KPI06_cuentaplame a
       --WHERE a.numruc NOT IN ( SELECT DISTINCT num_ruc FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion)
       EXCEPT ALL
       SELECT distinct num_ruc, PERIODO FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion
       
      ) WITH DATA NO PRIMARY INDEX ;

   /** REGISTROS QUE ESTAN EN FVIRTUAL Y NO EN MONGODB*/


       --DROP TABLE BDDWESTG.TMP_KPI06_DIF_FVIRNOMODB;
      CREATE MULTISET TABLE BDDWESTG.TMP_KPI06_DIF_FVIRNOMODB
      AS(

      SELECT distinct A.NUM_RUC, PERIODO FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion a
      --WHERE a.num_ruc NOT IN ( SELECT DISTINCT num_ruc FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion_MDB)
      EXCEPT ALL
      SELECT distinct num_ruc, PERIODO FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion_MDB

      ) WITH DATA NO PRIMARY INDEX ;



/************************ FIN GENERACION DE DIFERENCIAS  **********************/

/************************ GENERACION DE ARCHIVOS **************************/

  -- EN RECAUDA PERO NO EN FVIRTUAL
  --.EXPORT FILE /work1/teradata/dat/093168/DIF_K006012022_CAS127_TRANVSFVIR_20230206.unl;

  LOCK ROW FOR ACCESS
      SELECT * FROM BDDWESTG.TMP_KPI06_DIF_RECNOFVIR ORDER BY 1,2;

  -- EN FVIRTUAL PERO NO EN MONGODB
  --.EXPORT FILE /work1/teradata/dat/093168/DIF_K006022022_CAS127_FVIRVSMODB_20230206.unl;
 
   LOCK ROW FOR ACCESS
      SELECT * FROM BDDWESTG.TMP_KPI06_DIF_FVIRNOMODB ORDER BY 1,2;

 
/************************ FIN GENERACION DE ARCHIVOS **************************/

/************************ INSERTA CONTEOS A TABLAS DE DETALLE **************************/

  -- recauda vs fvirtual	VS MONGODB  
  -- BDDWESTG.TMP_KPI06_cuentaplame numruc,per_decla  
  -- BDDWESTG.TMP_KPI06_selecfvirtual_relacion   ruc declarante (num_ruc),doc declarado (num_doc),D/ND declara o no declara (ind_declara),periodo 
  -- BDDWESTG.TMP_KPI06_selecfvirtual_relacion_mdb   ruc declarante (num_ruc),doc declarado (num_doc),D/ND declara o no declara (ind_declara),num_ejercicio 

     -- BORRA REGISTROS CARGADOS EN LA MISMA FECHA
                    
        DELETE FROM BDDWESTG.T11908DETKPITRIBINT WHERE COD_KPI IN ('K006012022','K006022022') AND FEC_CARGA=CURRENT_DATE;

                    
     -- INSERTA CONTEO NO DECLARANTES RECAUDA Y FVIRTUAL
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','0',
       'K006012022',
       (SELECT COUNT(numruc) FROM BDDWESTG.TMP_KPI06_cuentaplame ),
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion where IND_DECLARA='ND' ),
       CURRENT_DATE);

     -- INSERTA CONTEO DECLARANTES RECAUDA Y FVIRTUAL
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','1',
       'K006012022',
       0,       
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion WHERE IND_DECLARA='D'),
       CURRENT_DATE);

     -- INSERTA CONTEO NO DECLARANTES FVIRTUAL Y MONGODB
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','0',
       'K006022022',
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion WHERE IND_DECLARA='ND'),
       (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion_MDB ),
       CURRENT_DATE);


     -- INSERTA CONTEO NO DECLARANTES FVIRTUAL Y MONGODB
     INSERT INTO BDDWESTG.T11908DETKPITRIBINT
     (COD_PER,IND_PRESDJ,COD_KPI,CNT_REGORIGEN,CNT_REGIDESTINO,FEC_CARGA) VALUES
     ('2022','1',
       'K006022022',
                            (SELECT COUNT(NUM_RUC) FROM BDDWESTG.TMP_KPI06_selecfvirtual_relacion WHERE IND_DECLARA='D'),
       0,
       CURRENT_DATE);



/************************ FIN INSERTA CONTEOS A TABLAS DE DETALLE **********************/
/************************ BORRA TABLAS TEMPORALES **********************/

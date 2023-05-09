DROP TABLE BDDWESTG.tmp093168_kpigr29_detruc_total;
DROP TABLE BDDWESTG.tmp093168_kpigr29_detruc_ciiu4;
DROP TABLE BDDWESTG.tmp093168_total_K029012022;
DROP TABLE BDDWESTG.tmp093168_dif_K029012022;


/*========================================================================================= */
/**********************************Contribuyentes TOTAL,CII4*********************************/
/*========================================================================================= */
------------------------Contribuyentes total Activos de Renta de 3ra excepto RUS -------------------------------


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr29_detruc_total
AS
(
SELECT  DISTINCT x0.ddp_numruc as num_ruc
FROM BDDWELND.ddp_ruc x0
INNER JOIN  BDDWELND.vfp_ruc x1 on x0.ddp_numruc=x1.vfp_numruc
WHERE x0.ddp_estado='00'
and x1.vfp_codtri in ('030301','033101','035101','034101','036101','031101','031201')
) WITH DATA UNIQUE PRIMARY INDEX( num_ruc);

-------------Cantidad Total de Contribuyentes Activos de Renta de 3ra excepto NRUS ----------------------

-------------con CIIU v4 actualizado en su Actividad Económica Principal -----------------------------


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr29_detruc_ciiu4
AS (
SELECT DISTINCT x0.num_ruc from BDDWELND.t5667acteco x0
INNER JOIN BDDWESTG.tmp093168_kpigr29_detruc_total x1 on x0.num_ruc=x1.num_ruc
WHERE x0.cod_tipact ='P' 
) WITH DATA UNIQUE PRIMARY INDEX (num_ruc);

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/

CREATE MULTISET TABLE BDDWESTG.tmp093168_total_K029012022
AS
(
    SELECT x0.num_ruc,x1.num_ruc as num_rucB
    FROM BDDWESTG.tmp093168_kpigr29_detruc_total x0
 FULL JOIN BDDWESTG.tmp093168_kpigr29_detruc_ciiu4 x1
    ON x0.num_ruc=x1.num_ruc
) WITH DATA NO PRIMARY INDEX;


CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K029012022 AS (
    SELECT * FROM BDDWESTG.tmp093168_total_K029012022 x0
    WHERE x0.num_rucB is null 
 )  WITH DATA PRIMARY INDEX (num_ruc);

/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K029012022'  AND FEC_CARGA=CURRENT_DATE;

INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
 SELECT 
  '2022',
   99,
  'K029012022',
  CURRENT_DATE,
  (select count(*) from BDDWESTG.tmp093168_kpigr29_detruc_total),
  (select count(*) from BDDWESTG.tmp093168_kpigr29_detruc_ciiu4),
   case when ((select count(*) from BDDWESTG.tmp093168_dif_K029012022)=0  and 
                  (select count(*) from BDDWESTG.tmp093168_kpigr29_detruc_total)<>0) 
                  then 1 else 0 end,
  (select count(*) from BDDWESTG.tmp093168_dif_K029012022),
        (select count(*) from BDDWESTG.tmp093168_total_K029012022 where num_ruc is null),
  (select count(*) from BDDWESTG.tmp093168_total_K029012022 where num_ruc=num_rucB)
 ;

/**********************************************************************************/
SELECT num_ruc FROM BDDWESTG.tmp093168_dif_K029012022 
 ORDER BY num_ruc;

/********************************************************************************/

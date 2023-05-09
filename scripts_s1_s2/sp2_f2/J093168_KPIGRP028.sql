DROP TABLE BDDWESTG.tmp093168_kpigr28_detruc_total;
DROP TABLE BDDWESTG.tmp093168_kpigr28_detruc_ciiu4;
DROP TABLE BDDWESTG.tmp093168_total_K028012022;
DROP TABLE BDDWESTG.tmp093168_dif_K028012022;

/*========================================================================================= */
/**********************************Contribuyentes TOTAL,CII4*********************************/
/*========================================================================================= */
------------------------Contribuyentes total-------------------------------


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr28_detruc_total
AS
(
SELECT  DISTINCT TRIM(ddp_numruc) as num_ruc FROM BDDWELND.DDP_RUC
) WITH DATA UNIQUE PRIMARY INDEX( num_ruc);

------------------------Contribuyentes CIIU 4 -----------------------------


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr28_detruc_ciiu4
AS (
SELECT DISTINCT TRIM(num_ruc) as num_ruc
FROM BDDWELND.t5667acteco WHERE cod_tipact='P'
) WITH DATA UNIQUE PRIMARY INDEX (num_ruc);

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/

CREATE MULTISET TABLE BDDWESTG.tmp093168_total_K028012022
AS
(
    SELECT x0.num_ruc,x1.num_ruc as num_rucB
    FROM BDDWESTG.tmp093168_kpigr28_detruc_total x0
 FULL JOIN BDDWESTG.tmp093168_kpigr28_detruc_ciiu4 x1
    ON x0.num_ruc=x1.num_ruc
) WITH DATA NO PRIMARY INDEX;


CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K028012022 AS (
    SELECT * FROM BDDWESTG.tmp093168_total_K028012022 x0
    WHERE x0.num_rucB is null 
 )  WITH DATA PRIMARY INDEX (num_ruc);

/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K028012022'  AND FEC_CARGA=CURRENT_DATE;

INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
 SELECT 
  '2022',
  99,
  'K028012022',
  CURRENT_DATE,
  (select count(*) from BDDWESTG.tmp093168_kpigr28_detruc_total),
  (select count(*) from BDDWESTG.tmp093168_kpigr28_detruc_ciiu4),
  case when ((select count(*) from BDDWESTG.tmp093168_dif_K028012022)=0  and 
                  (select count(*) from BDDWESTG.tmp093168_kpigr28_detruc_total)<>0)
                  then 1 else 0 end,
  (select count(*) from BDDWESTG.tmp093168_dif_K028012022),
        (select count(*) from BDDWESTG.tmp093168_total_K028012022 where num_ruc is null),
  (select count(*) from BDDWESTG.tmp093168_total_K028012022 where num_ruc=num_rucB)
 ;

/*****************************************************************************/
SELECT num_ruc FROM BDDWESTG.tmp093168_dif_K028012022 
 ORDER BY num_ruc;

/********************************************************************************/

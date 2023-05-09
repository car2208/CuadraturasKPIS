DROP TABLE BDDWESTG.tmp093168_kpigr33_mtobase_detextitf;
DROP TABLE BDDWESTG.tmp093168_kpigr33_cas406_426_djtot;
DROP TABLE BDDWESTG.tmp093168_total_K033012022;
DROP TABLE BDDWESTG.tmp093168_dif_K033012022;



/************************************************************************************************************/
---------------Cantidad de presentaciones en Recauda. t03djcab Y t04djdet. CAS 406 426.---------------------------

/************************************************************************************************************/


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr33_cas406_426_djtot
AS
(
    select x0.num_nabono,
           x0.cod_formul,
           x0.num_orden,
           x0.num_ruc,
           x0.cod_per,
           coalesce(MAX(CASE WHEN x1.num_cas='406' THEN trycast(x1.val_cas as decimal(25,4)) END),0) AS cas406,
           coalesce(MAX(CASE WHEN x1.num_cas='426' THEN trycast(x1.val_cas as decimal(25,4)) END),0) AS cas426,
          (cas406+cas426) val_cas
    from BDDWETB.t8593djcab x0
    inner join BDDWETB.t8594djdet x1 ON 
    x0.num_nabono=x1.num_nabono and 
    x0.cod_formul=x1.cod_formul and 
    x0.num_orden=x1.num_orden
    where x1.num_cas in('406','426') and
    x0.fec_finvig=2000101 and
    x1.fec_finvig=2000101 and
    x0.ind_deldwe='0' and
    x1.ind_deldwe='0' and
    x0.cod_per between '202201' and '202212' and
    x0.cod_formul='0695'
    GROUP BY 1,2,3,4,5
) WITH DATA UNIQUE PRIMARY INDEX (num_nabono,cod_formul,num_orden);

/************************************************************************************************************/
----------------Cantidad de presentaciones en el Recauda.T1391F695EXTITF. Monto Base -------------------------

/************************************************************************************************************/

CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr33_mtobase_detextitf
AS
(
select num_nabono,num_formul as cod_formul,num_orden,num_docdeclate,per_dec,sum(mto_base) as mto_base
from BDDWETB.t8477itfext x0
where x0.fec_finvig=2000101 and x0.ind_deldwe='0'
and x0.per_dec between '202201' and '202212'
and x0.num_formul='0695'
group by 1,2,3,4,5
) WITH DATA UNIQUE PRIMARY INDEX (num_nabono,cod_formul,num_orden);

-----------------------------------------------Diferencias ------------------------------------------------------

CREATE MULTISET TABLE BDDWESTG.tmp093168_total_K033012022
AS
(
SELECT 
     x0.num_nabono,
     x0.cod_formul,
     x0.num_orden,
     x0.num_ruc,
     x0.cod_per,
     x0.val_cas,
     x1.mto_base,
     x1.num_docdeclate as num_rucB
FROM BDDWESTG.tmp093168_kpigr33_cas406_426_djtot x0
FULL JOIN BDDWESTG.tmp093168_kpigr33_mtobase_detextitf x1 ON
x0.num_nabono=x1.num_nabono and
x0.cod_formul=x1.cod_formul and
x0.num_orden=x1.num_orden and
x0.num_ruc=x1.num_docdeclate and 
x0.cod_per=x1.per_dec and
x0.val_cas=x1.mto_base
) WITH DATA PRIMARY INDEX (num_nabono,cod_formul,num_orden);



CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K033012022 AS (
  SELECT x0.num_nabono,
         x0.cod_formul,
         x0.num_orden,
         x0.num_ruc,
         x0.cod_per,
         x0.val_cas  as val_mtobase_t04,
         x0.mto_base as val_mtobase_itfext
    FROM BDDWESTG.tmp093168_total_K033012022 x0
    WHERE x0.num_rucB is null 
 )  WITH DATA NO PRIMARY INDEX;

/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K033012022'  AND FEC_CARGA=CURRENT_DATE;

INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
 (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
 SELECT 
  '2022',
   99,
  'K033012022',
  CURRENT_DATE,
        (select count(*) from BDDWESTG.tmp093168_kpigr33_cas406_426_djtot),
        (select count(*) from BDDWESTG.tmp093168_kpigr33_mtobase_detextitf),
        case when ((select count(*) from BDDWESTG.tmp093168_dif_K033012022)=0 and
                  (select count(*) from BDDWESTG.tmp093168_kpigr33_cas406_426_djtot)<>0)
        then 1 else 0 end,
  (select count(*) from BDDWESTG.tmp093168_dif_K033012022),
        (select count(*) from BDDWESTG.tmp093168_total_K033012022 where num_ruc is null),
  (select count(*) from BDDWESTG.tmp093168_total_K033012022 where num_ruc=num_rucB);

/**********************************************************************************/
SELECT * FROM BDDWESTG.tmp093168_dif_K033012022 
 ORDER BY 3,4;

/********************************************************************************/

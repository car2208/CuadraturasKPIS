
DROP TABLE BDDWESTG.tmp093168_kpigr30_universocic;
DROP TABLE BDDWESTG.tmp093168_kpigr30_detcic_full;
DROP TABLE BDDWESTG.tmp093168_dif_K030012022;
/*========================================================================================= */
/**********************************Cantidad CIC Total   *************************************/
/*========================================================================================= */

CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr30_universocic AS (
select cod_fuente,cod_tipidenti,num_docidenti,fec_finvig,fec_inivig,cod_cic
,des_apepat,des_apemat,des_nompri,des_nomrazsoc,fec_nacimiento
from DWH_DATA.t2017identif
where fec_finvig=2000101
) WITH DATA NO PRIMARY INDEX;


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr30_detcic_full
AS
(

    with tmp093168_kpigr30_detcic_total as
    (
     select distinct cod_cic, des_nomrazsoc, fec_nacimiento
     from BDDWESTG.tmp093168_kpigr30_universocic
    ), tmp093168_kpigr30_group_desrazsoc as
    (
        select des_nomrazsoc, fec_nacimiento,count(*) as cant
        from tmp093168_kpigr30_detcic_total
        group by 1,2
        having cant>1
    ), tmp093168_kpigr30_group_cic as
    (
        select cod_cic,count(*) as cant
        from tmp093168_kpigr30_detcic_total
        group by 1
        having cant>1
    ) 
    select x0.*,
           case when x1.des_nomrazsoc is not null then 1
                 when x2.cod_cic is not null then 1
            else 0 end flag_error
    from   BDDWESTG.tmp093168_kpigr30_universocic x0
    left join tmp093168_kpigr30_group_desrazsoc x1 on  x0.des_nomrazsoc=x1.des_nomrazsoc and x0.fec_nacimiento=x1.fec_nacimiento
    left join tmp093168_kpigr30_group_cic x2 on x0.cod_cic=x2.cod_cic
) WITH DATA NO PRIMARY INDEX;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/

CREATE MULTISET TABLE BDDWESTG.tmp093168_dif_K030012022 AS (
    SELECT *
    FROM BDDWESTG.tmp093168_kpigr30_detcic_full
    WHERE flag_error=1
 )  WITH DATA NO PRIMARY INDEX;

/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
    WHERE COD_KPI='K030012022'  AND FEC_CARGA=CURRENT_DATE;

INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
    SELECT 
        '2022',
        99,
        'K030012022',
        CURRENT_DATE,
        (select count(*) from BDDWESTG.tmp093168_kpigr30_detcic_full),---denominador
        (select count(*) from BDDWESTG.tmp093168_kpigr30_detcic_full where flag_error=0),--numerador
        case when ((select count(*) from BDDWESTG.tmp093168_dif_K030012022)=0 and
                   (select count(*) from BDDWESTG.tmp093168_kpigr30_detcic_full)<>0)
        then 1 else 0 end,
        (select count(*) from  BDDWESTG.tmp093168_dif_K030012022),
         0,
        (select count(*) from BDDWESTG.tmp093168_kpigr30_detcic_full where flag_error=0)
    ;

/*******************************************************************************/
SELECT * FROM BDDWESTG.tmp093168_dif_K030012022 
 ORDER BY cod_cic;

/********************************************************************************/


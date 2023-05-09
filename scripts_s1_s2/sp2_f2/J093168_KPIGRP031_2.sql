SELECT 1 FROM  dbc.TablesV WHERE databasename = 'BDDWESTGD' AND TableName = 'tmp093168_kpigr31_02_cantorigen';

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr31_02_cantorigen
(
cnt_origen    varchar(20)
) NO PRIMARY INDEX;

/*========================================================================================= */
/***********Cantidad de registros en Teradata ITF Extornos  ******************************/
/*========================================================================================= */
SELECT 1 FROM  dbc.TablesV WHERE databasename = 'BDDWESTGD' AND TableName = 'tmp093168_kpigr31_02_cantdestino';

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr31_02_cantdestino
AS
(
    SELECT COUNT(1) as cnt_destino
    from BDDWETBD.T8477ITFEXT x0
    where x0.fec_finvig=2000101 and x0.ind_deldwe='0'
    and x0.per_dec between '202201' and '202212'
    and x0.num_formul='0695'
) WITH DATA NO PRIMARY INDEX;

/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
DELETE FROM BDDWEDQD.T11908DETKPITRIBINT 
    WHERE COD_KPI='K031022022'  AND FEC_CARGA=CURRENT_DATE;

INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
    SELECT 
        '2022',
        99,
        'K031022022',
        CURRENT_DATE,
        (select sum(cnt_origen) from BDDWESTGD.tmp093168_kpigr31_02_cantorigen),---denominador
        (select sum(cnt_destino) from BDDWESTGD.tmp093168_kpigr31_02_cantdestino)--numerador
    ;

/********************************************************************************/
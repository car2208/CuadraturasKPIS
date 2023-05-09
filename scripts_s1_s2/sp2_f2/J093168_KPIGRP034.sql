SELECT 1 FROM  dbc.TablesV WHERE databasename = 'BDDWESTGD' AND TableName = 'tmp093168_kpigr34_cantorigen';

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr34_cantorigen
(
cnt_origen    varchar(10)
) NO PRIMARY INDEX;

/*========================================================================================= */
/**********************************Cantidad CPE Baja Teradata  ******************************/
/*========================================================================================= */
SELECT 1 FROM  dbc.TablesV WHERE databasename = 'BDDWESTGD' AND TableName = 'tmp093168_kpigr34_cantdestino';

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr34_cantdestino
AS
(
    SELECT COUNT(*) as cnt_destino
    FROM BDDWETBD.t7630comprobcabe
    WHERE fec_finvig=2000101 and 
    ind_deldwe='0' and
    ind_estado_cpe='2' and
    COD_COMPROBANTE not in ('04','14')

) WITH DATA NO PRIMARY INDEX;

/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
DELETE FROM BDDWEDQD.T11908DETKPITRIBINT 
    WHERE COD_KPI='K034012022'  AND FEC_CARGA=CURRENT_DATE;

INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)--,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
    SELECT 
        '2022',
        99,
        'K034012022',
        CURRENT_DATE,
        (select sum(cnt_origen) from BDDWESTGD.tmp093168_kpigr34_cantorigen),---denominador
        (select sum(cnt_destino) from BDDWESTGD.tmp093168_kpigr34_cantdestino)--numerador
    ;

/********************************************************************************/
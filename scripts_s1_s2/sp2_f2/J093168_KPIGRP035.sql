SELECT 1 FROM  dbc.TablesV WHERE databasename = 'BDDWESTGD' AND TableName = 'tmp093168_kpigr35_origendestino';

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpigr35_origendestino
(
cnt_origen   varchar(20),
cnt_destino  varchar(20)
) NO PRIMARY INDEX;

/*========================================================================================= */
/**********************************Cantidad CPE Baja Teradata  ******************************/
/*========================================================================================= */
/****************************************************************************/
/********************INSERT EN TABLA FINAL***********************************/
DELETE FROM BDDWEDQD.T11908DETKPITRIBINT 
    WHERE COD_KPI='K035012022'  AND FEC_CARGA=CURRENT_DATE;

INSERT INTO BDDWEDQD.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)--,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
    SELECT 
        '2022',
        99,
        'K035012022',
        CURRENT_DATE,
        (select sum(cnt_origen)  from BDDWESTGD.tmp093168_kpigr35_origendestino),---denominador
        (select sum(cnt_destino) from BDDWESTGD.tmp093168_kpigr35_origendestino)--numerador
    ;

/********************************************************************************/
CREATE MULTISET TABLE bddwestg.T11908DETKPITRIBINT_NEW (

   COD_PER              VARCHAR(6)                     not null title 'Código que representa el periodo tributario',
   IND_PRESDJ           SMALLINT                       not null title 'Indicador si presentó DJ',
   COD_KPI              VARCHAR(10)                    title 'Código de KPI hijo',
   CNT_REGORIGEN        INTEGER                        title 'Conteo en el origen',
   CNT_REGIDESTINO      INTEGER                        title 'Conteo en el destino',
   MTO_REGORIGEN        DECIMAL(20, 2)                 title 'Suma de montos del origen',
   MTO_REGIDESTINO      DECIMAL(20, 2)                 title 'Suma de montos del destino',
   FEC_CARGA            DATE                           format 'YYYY-MM-DD' title 'Fecha de carga'
)
primary index PI_T11908(COD_PER,IND_PRESDJ,COD_KPI)
unique index IN01T11908(COD_PER,IND_PRESDJ,FEC_CARGA,COD_KPI);


INSERT INTO bddwestg.T11908DETKPITRIBINT_NEW
(cod_per,ind_presdj,cod_kpi,cnt_regorigen,cnt_regidestino,fec_carga)
select cod_per,
	   ind_presdj,
	   cod_kpi,
	   cnt_regorigen,
	   cnt_regidestino,
	   fec_carga
from bddwestg.T11908DETKPITRIBINT
;

RENAME TABLE bddwestg.T11908DETKPITRIBINT     TO bddwestg.T11908DETKPITRIBINT_OLD;
RENAME TABLE bddwestg.T11908DETKPITRIBINT_NEW TO bddwestg.T11908DETKPITRIBINT;

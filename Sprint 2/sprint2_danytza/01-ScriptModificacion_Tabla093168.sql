---Insertar primero en una temporal

drop index IN01T11908 on BDDWEDQD.T11908DETKPITRIBINT;

drop table BDDWEDQD.T11908DETKPITRIBINT;

/*==============================================================*/
/* Table: T11908DETKPITRIBINT                                   */
/*==============================================================*/
create multiset table BDDWEDQD.T11908DETKPITRIBINT (
   COD_PER              VARCHAR(6)                     not null title 'Código que representa el periodo tributario',
   IND_PRESDJ           SMALLINT                       not null title 'Indicador si presentó DJ',
   COD_KPI              VARCHAR(10)                    title 'Código de KPI hijo',
   CNT_REGORIGEN        INTEGER                        title 'Conteo en el origen',
   CNT_REGIDESTINO      INTEGER                        title 'Conteo en el destino',
   MTO_REGORIGEN        DECIMAL(20, 2)                 title 'Suma de montos del origen',
   MTO_REGIDESTINO      DECIMAL(20, 2)                 title 'Suma de montos del destino',
   FEC_CARGA            DATE                           format 'YYYY-MM-DD' title 'Fecha de carga'
)
   primary index PI_T11908(
      COD_PER,IND_PRESDJ,COD_KPI
   )
   unique index IN01T11908(
      COD_PER,IND_PRESDJ,FEC_CARGA,COD_KPI
   );

comment on table BDDWEDQD.T11908DETKPITRIBINT is 
'KPIS DETALLE TRIBUTOS INTERNOS ARCHIVO PERSONALIZADO.
Almacena el detalle de la informacion de los KPIS';
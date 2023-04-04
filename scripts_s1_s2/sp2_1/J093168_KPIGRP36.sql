/*
DROP TABLE bddwestg.tmp093168_kpi36_detafo_tr;
DROP TABLE bddwestg.tmp093168_kpi36_detafo_mdb;
DROP TABLE bddwestg.tmp093168_kpigr36_obs_cnorigen;
DROP TABLE bddwestg.tmp093168_kpigr36_cndestino1;
*/


DROP TABLE bddwestg.tmp093168_kpi_formularios;
create multiset table bddwestg.tmp093168_kpi_formularios 
(
  COD_FORM integer
);

INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1609);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1611);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1662);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1663);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1665);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1666);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1668);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1669);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1670);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1671);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1672);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1674);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1676);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(1683);
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(601 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(615 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(616 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(617 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(621 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(626 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(633 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(634 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(648 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(693 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(695 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(697 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(699 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(709 );
INSERT into bddwestg.tmp093168_kpi_formularios(cod_form) values(710 );


------------Genera Detalle Presentaciones Teradata-------------------------------


DROP TABLE bddwestg.tmp093168_kpi36_detafo_tr;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi36_detafo_tr
AS
(
select
    num_docidenti,
    num_nabono,
    cod_formul,
    num_orden,    
    fec_presenta,
    num_periodo
from desa_dwh_data.t2782djcab
where fec_presenta >= CAST('2022-10-01' AS DATE FORMAT 'YYYY-MM-DD')
and cod_formul in (SELECT CAST(cod_form as integer) FROM bddwestg.tmp093168_kpi_formularios)
and num_periodo between 202201 and 202213
)WITH DATA PRIMARY INDEX (num_nabono,cod_formul,num_orden);



DROP TABLE bddwestg.tmp093168_kpi36_detafo_mdb;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi36_detafo_mdb
AS
(
select 
    numRuc,
    numOperacion,
    codFormulario,
    numOrden,    
    cast(substr(trim(fecPresentacion),1,10) as date format 'yyyy-mm-dd') as fec_presenta,
    perPeriodo
 from bddwestg.present_mongo2 
 where fec_presenta >= CAST('2022-10-01' AS DATE FORMAT 'YYYY-MM-DD')
 and codFormulario in (SELECT cast(cod_form as integer) FROM bddwestg.tmp093168_kpi_formularios)
 and perPeriodo between '202201' and '202213'

)
WITH DATA PRIMARY INDEX (numOperacion,codFormulario,numOrden);



/*************************************************************************************/

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE bddwestg.tmp093168_kpigr36_obs_cnorigen;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr36_obs_cnorigen AS
(
    SELECT count(num_orden) as cant_comp_origen
    FROM bddwestg.tmp093168_kpi36_detafo_tr
) WITH DATA NO PRIMARY INDEX;


---------2. Conteo en MongoDB


DROP TABLE bddwestg.tmp093168_kpigr36_cndestino1;
CREATE MULTISET TABLE bddwestg.tmp093168_kpigr36_cndestino1 AS
(
    SELECT count(numOrden) as cant_comp_destino1
    FROM bddwestg.tmp093168_kpi36_detafo_mdb
) WITH DATA NO PRIMARY INDEX;



/***************************GENERA DETALLE DE DIFERENCIAS ***********************/
/********************************************************************************/

DROP TABLE bddwestg.tmp093168_total_K036012022 ;
CREATE MULTISET TABLE bddwestg.tmp093168_total_K036012022 AS ( 
    SELECT 
             x0.num_docidenti,
             x0.cod_formul,
             x0.num_orden,    
             x0.num_periodo,
             x1.numRuc as num_rucB
      FROM bddwestg.tmp093168_kpi36_detafo_tr x0
      FULL JOIN bddwestg.tmp093168_kpi36_detafo_mdb x1 ON
      x0.num_docidenti=x1.numRuc and 
      x0.cod_formul=x1.codFormulario and 
      x0.num_orden=x1.numOrden and 
      x0.num_periodo=x1.perPeriodo
) WITH DATA NO PRIMARY INDEX;


DROP TABLE bddwestg.tmp093168_dif_K036012022 ;
 CREATE MULTISET TABLE bddwestg.tmp093168_dif_K036012022 AS (
    SELECT 
          y0.num_docidenti,
          y0.cod_formul,
          y0.num_orden,    
          y0.num_periodo
    FROM bddwestg.tmp093168_total_K036012022 y0
    WHERE y0.num_rucB is null
   ) WITH DATA NO PRIMARY INDEX ;


/********************INSERT EN TABLA FINAL***********************************/
    
    DELETE FROM bddwestg.T11908DETKPITRIBINT 
    WHERE COD_KPI='K036012022'  AND FEC_CARGA=CURRENT_DATE;

    
    INSERT INTO bddwestg.T11908DETKPITRIBINT 
    (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO,IND_INCUNIV,CNT_REGDIF_OD,CNT_REGDIF_DO,CNT_REGCOINC)
    SELECT  '2022',
            '99',
            'K036012022',
            CURRENT_DATE,
            (SELECT cant_comp_origen FROM bddwestg.tmp093168_kpigr36_obs_cnorigen ),
            (SELECT cant_comp_destino1 FROM bddwestg.tmp093168_kpigr36_cndestino1 ),
            case when ((select count(*) from bddwestg.tmp093168_dif_K036012022)=0 and 
               (select count(*) from bddwestg.tmp093168_kpi36_detafo_tr)<>0)
   then 1 else 0 end,
         (select count(*) from bddwestg.tmp093168_dif_K036012022),
   (select count(*) from bddwestg.tmp093168_total_K036012022 where num_docidenti is null),
   (select count(*) from bddwestg.tmp093168_total_K036012022 where num_docidenti=num_rucB);


/******************************************************************************/


    LOCK ROW FOR ACCESS
    SELECT * FROM bddwestg.tmp093168_dif_K036012022
    ORDER BY 1,2,4;




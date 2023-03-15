/*
drop table bddwestg.tmp093168_kpi_formularios;
drop table bddwestg.tmp093168_kpi36_detafo_tr;
drop table bddwestg.tmp093168_kpi36_detafo_mdb;
drop table bddwestg.tmp093168_kpigr36_obs_cnorigen;
drop table bddwestg.tmp093168_kpigr36_cndestino1;
*/

create multiset table bddwestg.tmp093168_kpi_formularios 
(
  COD_FORM integer
);

INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1609);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1611);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1662);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1663);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1665);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1666);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1668);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1669);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1670);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1671);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1672);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1674);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1676);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(1683);
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(601 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(615 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(616 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(617 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(621 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(626 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(633 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(634 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(648 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(693 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(695 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(697 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(699 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(709 );
INSERT into BDDWESTG.tmp093168_kpi_formularios(cod_form) values(710 );


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
from dwh_data.t2782djcab
where fec_presenta >= CAST('2022-10-01' AS DATE FORMAT 'YYYY-MM-DD')
and cod_formul in (SELECT CAST(cod_form as integer) FROM bddwestg.tmp093168_kpi_formularios)
and num_periodo between 202201 and 202213
)WITH DATA PRIMARY INDEX (num_nabono,cod_formul,num_orden);


------------Genera Detalle Presentaciones MongoDB-------------------------------



CREATE MULTISET TABLE bddwestg.tmp093168_kpi36_detafo_mdb
AS
(
select 
    numRuc,
    numOperacion,
    cast(codFormulario as smallint) as codFormulario,
    numOrden,    
    cast(substr(trim(fecPresentacion),1,10) as date format 'yyyy-mm-dd') as fec_presenta,
    cast(perPeriodo as integer) as perPeriodo
 from bddwestg.present_mongo2 
 where fec_presenta >= CAST('2022-10-01' AS DATE FORMAT 'YYYY-MM-DD')
 and codFormulario in (SELECT cast(cod_form as integer) FROM bddwestg.tmp093168_kpi_formularios)
 and perPeriodo between '202201' and '202213'

)
WITH DATA PRIMARY INDEX (numOperacion,codFormulario,numOrden);


/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

CREATE MULTISET TABLE bddwestg.tmp093168_kpigr36_obs_cnorigen AS
(
    SELECT count(num_orden) as cant_comp_origen
    FROM bddwestg.tmp093168_kpi36_detafo_tr
) WITH DATA NO PRIMARY INDEX;


---------2. Conteo en FVirtual


CREATE MULTISET TABLE bddwestg.tmp093168_kpigr36_cndestino1 AS
(
    SELECT count(numOrden) as cant_comp_destino1
    FROM bddwestg.tmp093168_kpi36_detafo_mdb
) WITH DATA NO PRIMARY INDEX;


      SELECT
            *
      FROM bddwestg.tmp093168_kpi36_detafo_tr x0
      INNER JOIN bddwestg.tmp093168_kpi36_detafo_mdb x1
      ON 
      x0.num_docidenti=x1.numRuc and
      x0.cod_formul=x1.codFormulario and
      x0.num_orden=x1.numOrden and
      x0.fec_presenta=x1.fec_presenta


/***************************GENERA DETALLE DE DIFERENCIAS ***********************/
/********************************************************************************/

DROP TABLE ${BD_STG}.tmp093168_dif_${KPI_01};

	CREATE MULTISET TABLE ${BD_STG}.tmp093168_dif_${KPI_01} AS (
    SELECT 
          y0.num_docidenti,
          y0.num_nabono,
          y0.cod_formul,
          y0.num_orden,    
          y0.num_periodo
    FROM
    (
      SELECT
            *
      FROM bddwestg.tmp093168_kpi36_detafo_tr x0
      INNER JOIN bddwestg.tmp093168_kpi36_detafo_mdb x1
      ON 
      x0.num_docidenti=x1.numRuc and
      x0.cod_formul=x1.codFormulario and
      x0.num_orden=x1.numOrden and
      x0.fec_presenta=x1.fec_presenta



     ) y0
   ) WITH DATA NO PRIMARY INDEX ;

 .IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/********************INSERT EN TABLA FINAL***********************************/
    
DELETE FROM bddwestg.T11908DETKPITRIBINT  WHERE FEC_CARGA=CURRENT_DATE AND COD_KPI ='K036012022';
     
INSERT INTO bddwestg.T11908DETKPITRIBINT 
(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
SELECT  '2022',
        '99',
        'K036012022',
         CURRENT_DATE,
         (SELECT cant_comp_origen FROM bddwestg.tmp093168_kpigr36_obs_cnorigen ),
         (SELECT cant_comp_destino1 FROM bddwestg.tmp093168_kpigr36_cndestino1 );


/**********Este KPI no tiene archivo de diferencias************/
/*========================================================================================= */
/***********************************TRANSACCIONALES******************************************/
/*========================================================================================= */
/*******************Extrae recibos por honorarios******************/

DROP TABLE bddwestg.tmp093168_cantrecibos_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_cantrecibos_pilot as
(
	SELECT distinct num_ruc ,num_serie ,cod_tipcomp ,num_comprob
	FROM bddwestg.t3639recibo
	WHERE EXTRACT(YEAR FROM fec_emision_rec) = 2022
	--AND FEC_EMISION_REC <= DATE '2022-10-27'
	AND FEC_EMISION_REC <= DATE '2023-02-19'
	AND ind_estado_rec = '0'
	AND cod_tipcomp = '01'
) WITH DATA NO PRIMARY INDEX;

DROP TABLE bddwestg.tmp093168_cantnotascredito_pilot ;
CREATE MULTISET TABLE bddwestg.tmp093168_cantnotascredito_pilot as
(
	SELECT distinct num_ruc ,num_serie ,'07' as cod_tipcomp ,num_nota as num_comprob
	FROM bddwestg.t3634notacredito 
	WHERE EXTRACT(YEAR FROM fec_emision_nc) = 2022
	AND ind_estado_nc = '0'
	AND cod_tipcomp_ori = '01'
	AND fec_emision_nc  <= DATE '2023-02-19'
) WITH DATA NO PRIMARY INDEX;


/*******************Última DJ******************/

DROP TABLE bddwestg.tmp093168_udjkpi1_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_udjkpi1_pilot as
(
SELECT t2.num_nabono as t03nabono,
        t2.num_orden as t03norden,
        t2.cod_formul as t03formulario,
        t2.num_ruc as  t03lltt_ruc,
         t2.cod_per as t03periodo,
         t2.fec_presenta as t03f_presenta 
FROM 
		(
			SELECT 
			    cod_per ,
				num_ruc ,
				cod_formul ,
				MAX(fec_presenta) as fec_presenta,
				MAX(num_resumen) as num_resumen,
				MAX(num_orden) as num_orden 
			FROM bddwetb.t8593djcab 
			WHERE cod_formul = '0616' 
			AND cod_per between '202201' and '202212'
			AND fec_presenta <= DATE '2023-02-19'
			GROUP BY 1,2,3
) t1			
INNER JOIN bddwetb.t8593djcab t2 ON t2.cod_per = t1.cod_per 
AND t2.num_ruc = t1.num_ruc
AND t2.cod_formul = t1.cod_formul
AND t2.fec_presenta = t1.fec_presenta
AND t2.num_resumen = t1.num_resumen
AND t2.num_orden = t1.num_orden
)
WITH DATA PRIMARY INDEX (t03nabono,t03formulario,t03norden,t03periodo);


/*********Extrae RxHe de Form 0616 , útlima dj***********/
 
DROP TABLE bddwestg.tmp093168_cantrecibosf616_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_cantrecibosf616_pilot as
(
SELECT DISTINCT x0.num_docide_dec,x0.num_serie_cp,x0.tip_cp,CAST(x0.num_cp AS INTEGER) AS num_cp
FROM bddwestg.t1209f616rddet x0
INNER JOIN bddwestg.tmp093168_udjkpi1_pilot x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '202201' and '202212'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '02'
AND substr(x0.num_serie_cp,1,1) ='E'
)
WITH DATA NO PRIMARY INDEX;

DROP TABLE bddwestg.tmp093168_cantnotcredtf616_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_cantnotcredtf616_pilot as
(
SELECT DISTINCT x0.num_docide_dec,x0.num_serie_cp,x0.tip_cp,CAST(x0.num_cp AS INTEGER) AS num_cp
FROM bddwestg.t1209f616rddet x0
INNER JOIN bddwestg.tmp093168_udjkpi1_pilot x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '202201' and '202212'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '07'
AND substr(x0.num_serie_cp,1,1) ='E'
)
WITH DATA NO PRIMARY INDEX;

/******Union de RxH de CPE y Form 0616**************/

DROP TABLE bddwestg.tmp093168_detcantrxhe_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_detcantrxhe_pilot as
(
	SELECT  TRIM(num_ruc) AS num_ruc,
	        TRIM(num_serie) as num_serie,
	        '02' cod_tipcomp,num_comprob 
	FROM bddwestg.tmp093168_cantrecibos_pilot
	UNION
	SELECT TRIM(num_docide_dec),
		   TRIM(num_serie_cp),
		   TRIM(tip_cp),
		   num_cp 
	FROM bddwestg.tmp093168_cantrecibosf616_pilot
	UNION 
	SELECT  TRIM(num_ruc) AS num_ruc,
	        TRIM(num_serie) as num_serie,
	        cod_tipcomp,
			num_comprob 
	FROM bddwestg.tmp093168_cantnotascredito_pilot
	UNION
	SELECT
		   TRIM(num_docide_dec),
		   TRIM(num_serie_cp),
		   TRIM(tip_cp),
		   num_cp 
	FROM bddwestg.tmp093168_cantnotcredtf616_pilot
)
WITH DATA NO PRIMARY INDEX;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/


-------1. Detalle de RxHe en transaccional

DROP TABLE bddwestg.tmp093168_detcantrxhetr_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_detcantrxhetr_pilot
AS(
	SELECT DISTINCT x0.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
			x0.num_serie,x0.cod_tipcomp,x0.num_comprob
	FROM bddwestg.tmp093168_detcantrxhe_pilot x0
	LEFT JOIN bddwestg.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
	WHERE substr(x0.num_ruc,1,1) <>'2' or  x0.num_ruc in (select num_ruc from bddwestg.tmp093168_rucs20_incluir)
) WITH DATA NO PRIMARY INDEX ; 

------2. Detalle de RxHe en Archivo Personalizado Fvirtual

DROP TABLE bddwestg.tmp093168_detcantrxhefv_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_detcantrxhefv_pilot
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_serie,x0.tip_comp,x0.num_comp
	FROM bddwestg.t5373cas107 x0
	INNER JOIN bddwestg.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.tip_comp in ('02','07')
	AND SUBSTR(x0.num_serie,1,1) = 'E'
) WITH DATA NO PRIMARY INDEX ; 


-------3. Detalle de RxHe en Archivo Personalizado MongoDB

DROP TABLE bddwestg.tmp093168_detcantrxhemdb_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_detcantrxhemdb_pilot
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_serie,x0.COD_TIPCOMP,x0.num_comp
	FROM bddwestg.T5373CAS107_MONGODB x0
	INNER JOIN bddwestg.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.COD_TIPCOMP in ('02','07')
	AND SUBSTR(x0.num_serie,1,1) = 'E'
) WITH DATA NO PRIMARY INDEX ; 


/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE bddwestg.tmp093168_kpi01_cnorigen_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi01_cnorigen_pilot AS
(
	SELECT ind_presdj,count(num_comprob) as cant_rxh_origen
	FROM bddwestg.tmp093168_detcantrxhetr_pilot
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE bddwestg.tmp093168_kpi01_cndestino1_pilot;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi01_cndestino1_pilot AS
(
	SELECT ind_presdj,count(num_comp) as cant_rxh_destino1
	FROM bddwestg.tmp093168_detcantrxhefv_pilot
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE bddwestg.tmp093168_kpi02_cndestino2_pilot	;
CREATE MULTISET TABLE bddwestg.tmp093168_kpi02_cndestino2_pilot AS
(
	SELECT ind_presdj,count(num_comp) as cant_rxh_destino2
	FROM bddwestg.tmp093168_detcantrxhemdb_pilot
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;



/********************INSERT EN TABLA FINAL***********************************/

  DELETE FROM bddwestg.T11908DETKPITRIBINT WHERE COD_KPI='K001P12022'  AND FEC_CARGA=CURRENT_DATE;
  

	INSERT INTO bddwestg.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 2022,z.ind_presdj,
	       'K001P12022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT --x0.num_ruc,
			       x0.ind_presdj,
			       case when x0.ind_presdj=0 then (select coalesce(sum(cant_rxh_origen),0) from bddwestg.tmp093168_kpi01_cnorigen_pilot) else 0 end as cant_origen,
			       coalesce(x1.cant_rxh_destino1,0) as cant_destino
			FROM (
				select y.ind_presdj,SUM(y.cant_rxh_origen) as cant_rxh_origen
				from
				(
					select * from bddwestg.tmp093168_kpi01_cnorigen_pilot
					union all select 1,0 from (select '1' agr1) a
					union all select 0,0 from (select '0' agr0) b
				) y group by 1

			) x0
			LEFT JOIN bddwestg.tmp093168_kpi01_cndestino1_pilot x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;


DELETE FROM bddwestg.T11908DETKPITRIBINT WHERE COD_KPI='K001P22022'  AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO bddwestg.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  2022,z.ind_presdj,
	        'K001P22022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT 
			       x0.ind_presdj,
			       x0.cant_rxh_destino1 AS cant_origen,
			       case when x0.ind_presdj=0  then (select coalesce(sum(cant_rxh_destino2),0) from bddwestg.tmp093168_kpi02_cndestino2_pilot) else 0 end AS cant_destino
			FROM (
				select y.ind_presdj,SUM(y.cant_rxh_destino1) as cant_rxh_destino1
				from
				(
					select * from bddwestg.tmp093168_kpi01_cndestino1_pilot
					union all select 1,0 from (select '1' agr1) a
					union all select 0,0 from (select '0' agr0) b
				) y group by 1
			) x0
			LEFT JOIN bddwestg.tmp093168_kpi02_cndestino2_pilot x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/	
	
	DROP TABLE bddwestg.DIF_K001012022_pilot;
	CREATE MULTISET TABLE bddwestg.DIF_K001012022_pilot
	AS
	(
	SELECT   DISTINCT
		      y0.num_ruc as num_ruc_trab,
			  y0.num_serie,
			  y0.cod_tipcomp,
			  y0.num_comprob
	FROM
	(
		SELECT num_ruc,num_serie,cod_tipcomp,num_comprob 
		FROM bddwestg.tmp093168_detcantrxhetr_pilot
		EXCEPT ALL
		SELECT num_ruc,num_serie,tip_comp,cast(num_comp as integer)
		FROM bddwestg.tmp093168_detcantrxhefv_pilot
	) y0
	) WITH DATA NO PRIMARY INDEX;


	DROP TABLE bddwestg.DIF_K001022022_pilot;
	CREATE MULTISET TABLE bddwestg.DIF_K001022022_pilot
	AS
	(
	SELECT   DISTINCT 
		      y0.num_ruc as num_ruc_trab,
			  y0.num_serie,
			  y0.tip_comp  as cod_tipcomp,
			  y0.num_comprob
	FROM
	(
		SELECT num_ruc,num_serie,tip_comp,cast(num_comp as integer)  as num_comprob
		FROM bddwestg.tmp093168_detcantrxhefv_pilot
		EXCEPT ALL
		SELECT num_ruc,num_serie,cod_tipcomp,cast(num_comp as integer) 
		FROM bddwestg.tmp093168_detcantrxhemdb_pilot
	) y0
	) WITH DATA NO PRIMARY INDEX;


/********************************************************************************/
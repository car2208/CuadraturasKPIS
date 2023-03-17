/*========================================================================================= */
/***********************************TRANSACCIONALES******************************************/
/*========================================================================================= */
/*******************Extrae recibos por honorarios******************/

DROP TABLE BDDWESTGD.tmp093168_cantrecibos;

CREATE MULTISET TABLE BDDWESTGD.tmp093168_cantrecibos as
(
	SELECT distinct num_ruc ,num_serie ,cod_tipcomp ,num_comprob
	FROM BDDWESTGD.t3639recibo_2
	WHERE EXTRACT(YEAR FROM fec_emision_rec) = 2022
	AND ind_estado_rec = '0'
	AND cod_tipcomp = '01'
) WITH DATA NO PRIMARY INDEX;

/*******************Última DJ******************/

DROP TABLE BDDWESTGD.tmp093168_udjkpi1;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_udjkpi1 as
(
SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,t2.t03lltt_ruc,
         t2.t03periodo,t2.t03f_presenta 
FROM 
		(
			SELECT 
			t03periodo,
			t03lltt_ruc,
			t03formulario,
			MAX(t03f_presenta) as t03f_presenta,
			MAX(t03nresumen) as t03nresumen 
			FROM BDDWESTGD.t03djcab_2
			WHERE t03formulario = '0616' 
			AND substr(t03periodo,1,4)=2022
		    GROUP BY 1,2,3
		    
		) t1
INNER JOIN BDDWESTGD.t03djcab_2 t2 ON t2.t03periodo = t1.t03periodo 
AND t2.t03lltt_ruc = t1.t03lltt_ruc
AND t2.t03formulario = t1.t03formulario
AND t2.t03f_presenta = t1.t03f_presenta
AND t2.t03nresumen = t1.t03nresumen
)
WITH DATA NO PRIMARY INDEX;

/*********Extrae RxHe de Form 0616 , útlima dj***********/
 
DROP TABLE BDDWESTGD.tmp093168_cantrecibosf616;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_cantrecibosf616 as
(
SELECT DISTINCT num_docide_dec,num_serie_cp,tip_cp,CAST(num_cp AS INTEGER) AS num_cp
FROM BDDWESTGD.t1209f616rddet_2 x0, BDDWESTGD.tmp093168_udjkpi1 x1
WHERE substr(t03periodo,1,4)=2022
AND t03formulario = '0616'
AND tip_docide_dec = '6'
AND tip_cp = '02'
AND substr(num_serie_cp,1,1) ='E'
AND num_paq = t03nabono
AND formulario = t03formulario
AND norden = t03norden
AND per_periodo = t03periodo
)
WITH DATA NO PRIMARY INDEX;


/******Union de RxH de CPE y Form 0616**************/

DROP TABLE BDDWESTGD.tmp093168_detcantrxhe;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_detcantrxhe as
(
	SELECT num_ruc,num_serie,cod_tipcomp,num_comprob FROM BDDWESTGD.tmp093168_cantrecibos
	UNION
	SELECT num_docide_dec,num_serie_cp,tip_cp,num_cp FROM BDDWESTGD.tmp093168_cantrecibosf616
)
WITH DATA NO PRIMARY INDEX;


/*========================================================================================= */
/**********************************ARCHIVO PERSONALIZADO************************************/
/*========================================================================================= */

/**********Determina Indicador de presentación de DJ Anual ****************/

DROP TABLE tmp093168_kpiperson_dj1;
CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj1 as
(
SELECT num_ruc,MAX(num_sec) as num_sec
FROM BDDWESTGD.t5847ctldecl_2 
WHERE num_ejercicio = 2022
AND num_formul = '0709' 
AND ind_actual = '1' 
AND ind_estado = '0' 
AND ind_proceso = '1'
GROUP BY 1
) with data no primary INDEX ON COMMIT PRESERVE ROWS
;

------------1. Sí presentaron ----------------------

DROP TABLE tmp093168_kpiperson_dj2;
CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj2 as
(
SELECT num_ruc,MAX(num_sec) as num_sec 
FROM BDDWESTGD.t5847ctldecl_2 
WHERE num_ejercicio = 2022
AND num_formul = '0709' 
AND ind_estado = '2'
GROUP BY 1
)  with data no primary INDEX ON COMMIT PRESERVE ROWS
;

------------2. No presentaron-------------------------

DROP TABLE tmp093168_kpiperson_sindj;

CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_sindj as (
SELECT num_ruc, num_sec FROM tmp093168_kpiperson_dj1 
WHERE num_ruc NOT IN ( SELECT num_ruc FROM tmp093168_kpiperson_dj2)
)  WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS
;

------------3. Consolida Indicador -------------------

DROP TABLE BDDWESTGD.tmp093168_kpiperindj;

CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpiperindj as (
SELECT num_ruc, num_sec,1 as ind_presdj FROM tmp093168_kpiperson_dj1 
WHERE num_ruc NOT IN ( SELECT num_ruc FROM tmp093168_kpiperson_dj2)
UNION ALL
SELECT num_ruc,num_sec,0 FROM tmp093168_kpiperson_dj2
)
 WITH DATA PRIMARY INDEX (num_sec)
;


/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/


-------1. Detalle de RxHe en transaccional

DROP TABLE BDDWESTGD.tmp093168_detcantrxhetr;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_detcantrxhetr
AS(
	SELECT DISTINCT x0.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
			x0.num_serie,x0.cod_tipcomp,x0.num_comprob
	FROM BDDWESTGD.tmp093168_detcantrxhe x0
	LEFT JOIN BDDWESTGD.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
) WITH DATA NO PRIMARY INDEX ; 


-------2. Detalle de RxHe en Archivo Personalizado Fvirtual
DROP TABLE BDDWESTGD.tmp093168_detcantrxhefv;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_detcantrxhefv
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_serie,x0.tip_comp,x0.num_comp
	FROM BDDWESTGD.t5373cas107 x0
	LEFT JOIN BDDWESTGD.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.tip_comp = '02'
	AND x0.tip_doc = '06'
	AND SUBSTR(x0.num_serie,1,4) = 'E'
) WITH DATA NO PRIMARY INDEX ; 

-------3. Detalle de RxHe en Archivo Personalizado MongoDB

DROP TABLE BDDWESTGD.tmp093168_detcantrxhemdb;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_detcantrxhemdb
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_serie,x0.COD_TIPCOMP,x0.num_comp
	FROM BDDWESTGD.T5373CAS107_MONGODB x0
	LEFT JOIN BDDWESTGD.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.COD_TIPCOMP = '02'
	AND x0.COD_TIPDOC = '06'
	AND SUBSTR(x0.num_serie,1,4) = 'E'
) WITH DATA NO PRIMARY INDEX ; 

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTGD.tmp093168_kpi01_cnorigen;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi01_cnorigen AS
(
	SELECT num_ruc,ind_presdj,count(num_comprob) as cant_rxh_origen
	FROM BDDWESTGD.tmp093168_detcantrxhetr
	GROUP BY 1,2
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE BDDWESTGD.tmp093168_kpi01_cndestino1;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi01_cndestino1 AS
(
	SELECT num_ruc,ind_presdj,count(num_comp) as cant_rxh_destino1
	FROM BDDWESTGD.tmp093168_detcantrxhefv
	GROUP BY 1,2
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE BDDWESTGD.tmp093168_kpi02_cndestino2	;
CREATE MULTISET TABLE BDDWESTGD.tmp093168_kpi02_cndestino2 AS
(
	SELECT num_ruc,ind_presdj,count(num_comp) as cant_rxh_destino2
	FROM BDDWESTGD.tmp093168_detcantrxhemdb
	GROUP BY 1,2
) WITH DATA NO PRIMARY INDEX;



/********************INSERT EN TABLA FINAL***********************************/

	INSERT INTO BDDWEDQD.TXXXXDETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 2022,z.ind_presdj,
	       'XXXXX',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT x0.num_ruc,
			       x0.ind_presdj,
			       cant_rxh_origen as cant_origen,
			       coalesce(x1.cant_rxh_destino1,0) as cant_destino
			FROM BDDWESTGD.tmp093168_kpi01_cnorigen x0
			LEFT JOIN BDDWESTGD.tmp093168_kpi01_cndestino1 x1 
			ON x0.num_ruc=x1.num_ruc AND x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

	INSERT INTO BDDWEDQD.TXXXXDETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  2022,z.ind_presdj,
	        'XXXX2',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT x0.num_ruc,
			       x0.ind_presdj,
			       x0.cant_rxh_destino1 AS cant_origen,
			       coalesce(x1.cant_rxh_destino2,0) AS cant_destino
			FROM BDDWESTGD.tmp093168_kpi01_cndestino1 x0
			LEFT JOIN BDDWESTGD.tmp093168_kpi02_cndestino2 x1 
			ON x0.num_ruc=x1.num_ruc AND x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/	
	

	SELECT   'KPI0012022',
		      y0.num_ruc,
		      y0.ind_presdj,
			  y0.num_serie,
			  y0.cod_tipcomp,
			  y0.num_comprob
	FROM
	(
		SELECT num_ruc,ind_presdj,num_serie,cod_tipcomp,num_comprob 
		FROM BDDWESTGD.tmp093168_detcantrxhetr
		EXCEPT ALL
		SELECT num_ruc,ind_presdj,num_serie,tip_comp,cast(num_comp as integer)
		FROM BDDWESTGD.tmp093168_detcantrxhefv
	) y0;


	SELECT   'KPI0022022',
		      y0.num_ruc,
		      y0.ind_presdj,
			  y0.num_serie,
			  y0.tip_comp  as cod_tipcomp,
			  y0.num_comprob
	FROM
	(
		SELECT num_ruc,ind_presdj,num_serie,tip_comp,cast(num_comp as integer)  as num_comprob
		FROM BDDWESTGD.tmp093168_detcantrxhefv
		EXCEPT ALL
		SELECT num_ruc,ind_presdj,num_serie,cod_tipcomp,cast(num_comp as integer) 
		FROM BDDWESTGD.tmp093168_detcantrxhemdb
	) y0;

/********************************************************************************/
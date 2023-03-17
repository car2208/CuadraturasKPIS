/*========================================================================================= */
/***********************************TRANSACCIONALES******************************************/
/*========================================================================================= */
/*******************Extrae recibos por honorarios******************/

DROP TABLE BDDWESTG.tmp093168_cantrecibos;
CREATE MULTISET TABLE BDDWESTG.tmp093168_cantrecibos as
(
	SELECT distinct num_ruc ,num_serie ,cod_tipcomp ,num_comprob
	FROM BDDWESTG.t3639recibo
	WHERE EXTRACT(YEAR FROM fec_emision_rec) = 2022
	--AND FEC_EMISION_REC <= DATE '2022-10-27'
	AND FEC_EMISION_REC <= DATE '2023-02-27'
	AND ind_estado_rec = '0'
	AND cod_tipcomp = '01'
) WITH DATA NO PRIMARY INDEX;

select count(*) from BDDWESTG.tmp093168_cantrecibos;


CREATE MULTISET TABLE BDDWESTG.tmp093168_cantnotascredito as
(
	SELECT distinct num_ruc ,num_serie ,'07' as cod_tipcomp ,num_nota as num_comprob
	FROM BDDWESTG.t3634notacredito 
	WHERE EXTRACT(YEAR FROM fec_emision_nc) = 2022
	AND ind_estado_nc = '0'
	AND cod_tipcomp_ori = '01'
	AND fec_emision_nc  <= DATE '2023-02-27'
) WITH DATA NO PRIMARY INDEX;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/*******************Última DJ******************/



DROP TABLE BDDWESTG.tmp093168_udjkpi1;
CREATE MULTISET TABLE BDDWESTG.tmp093168_udjkpi1 as
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
			MAX(t03nresumen) as t03nresumen,
			MAX(t03norden) as t03norden 
			FROM BDDWESTG.t03djcab
			WHERE t03formulario = '0616' 
			AND t03periodo between '202201' and '202212'
			--AND t03f_presenta <= DATE '2022-10-27'
			AND t03f_presenta <= DATE '2023-02-27'
		    GROUP BY 1,2,3
		    
		) t1
INNER JOIN BDDWESTG.t03djcab t2 ON t2.t03periodo = t1.t03periodo 
AND t2.t03lltt_ruc = t1.t03lltt_ruc
AND t2.t03formulario = t1.t03formulario
AND t2.t03f_presenta = t1.t03f_presenta
AND t2.t03nresumen = t1.t03nresumen
AND t2.t03norden = t1.t03norden
)
WITH DATA PRIMARY INDEX (t03nabono,t03formulario,t03norden,t03periodo);


/*********Extrae RxHe de Form 0616 , útlima dj***********/
 
DROP TABLE BDDWESTG.tmp093168_cantrecibosf616;
CREATE MULTISET TABLE BDDWESTG.tmp093168_cantrecibosf616 as
(
SELECT DISTINCT x0.num_docide_dec,x0.num_serie_cp,x0.tip_cp,CAST(x0.num_cp AS INTEGER) AS num_cp
FROM BDDWESTG.t1209f616rddet x0
INNER JOIN BDDWESTG.tmp093168_udjkpi1 x1 ON 
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

DROP TABLE BDDWESTG.tmp093168_cantnotcredtf616;
CREATE MULTISET TABLE BDDWESTG.tmp093168_cantnotcredtf616 as
(
SELECT DISTINCT x0.num_docide_dec,x0.num_serie_cp,x0.tip_cp,CAST(x0.num_cp AS INTEGER) AS num_cp
FROM BDDWESTG.t1209f616rddet x0
INNER JOIN BDDWESTG.tmp093168_udjkpi1 x1 ON 
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

DROP TABLE BDDWESTG.tmp093168_detcantrxhe;
CREATE MULTISET TABLE BDDWESTG.tmp093168_detcantrxhe as
(
	SELECT  TRIM(num_ruc) AS num_ruc,
	        TRIM(num_serie) as num_serie,
	        '02' cod_tipcomp,num_comprob 
	FROM BDDWESTG.tmp093168_cantrecibos
	UNION
	SELECT TRIM(num_docide_dec),
		   TRIM(num_serie_cp),
		   TRIM(tip_cp),
		   num_cp 
	FROM BDDWESTG.tmp093168_cantrecibosf616
	UNION 
	SELECT  TRIM(num_ruc) AS num_ruc,
	        TRIM(num_serie) as num_serie,
	        cod_tipcomp,
			num_comprob 
	FROM BDDWESTG.tmp093168_cantnotascredito
	UNION
	SELECT
		   TRIM(num_docide_dec),
		   TRIM(num_serie_cp),
		   TRIM(tip_cp),
		   num_cp 
	FROM BDDWESTG.tmp093168_cantnotcredtf616
)
WITH DATA NO PRIMARY INDEX;

/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/


-------1. Detalle de RxHe en transaccional

DROP TABLE BDDWESTG.tmp093168_detcantrxhetr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_detcantrxhetr
AS(
	SELECT DISTINCT x0.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
			x0.num_serie,x0.cod_tipcomp,x0.num_comprob
	FROM BDDWESTG.tmp093168_detcantrxhe x0
	LEFT JOIN BDDWESTG.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
	WHERE substr(x0.num_ruc,1,1) <>'2' or  x0.num_ruc in (select num_ruc from BDDWESTG.tmp093168_rucs20_incluir)
) WITH DATA NO PRIMARY INDEX ; 

select count(*) from BDDWESTG.tmp093168_detcantrxhetr;

-------2. Detalle de RxHe en Archivo Personalizado Fvirtual

DROP TABLE BDDWESTG.tmp093168_detcantrxhefv;
CREATE MULTISET TABLE BDDWESTG.tmp093168_detcantrxhefv
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_serie,x0.tip_comp,x0.num_comp
	FROM BDDWESTG.t5373cas107 x0
	INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.tip_comp in ('02','07')
	AND SUBSTR(x0.num_serie,1,1) = 'E'
) WITH DATA NO PRIMARY INDEX ; 

select count(*) from BDDWESTG.tmp093168_detcantrxhefv;

-------3. Detalle de RxHe en Archivo Personalizado MongoDB

DROP TABLE BDDWESTG.tmp093168_detcantrxhemdb;
CREATE MULTISET TABLE BDDWESTG.tmp093168_detcantrxhemdb
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_serie,x0.COD_TIPCOMP,x0.num_comp
	FROM BDDWESTG.T5373CAS107_MONGODB x0
	INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.COD_TIPCOMP in ('02','07')
	AND SUBSTR(x0.num_serie,1,1) = 'E'
) WITH DATA NO PRIMARY INDEX ; 

select count(*) from BDDWESTG.tmp093168_detcantrxhemdb;

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

DROP TABLE BDDWESTG.tmp093168_kpi01_cnorigen;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi01_cnorigen AS
(
	SELECT ind_presdj,count(num_comprob) as cant_rxh_origen
	FROM BDDWESTG.tmp093168_detcantrxhetr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
DROP TABLE BDDWESTG.tmp093168_kpi01_cndestino1;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi01_cndestino1 AS
(
	SELECT ind_presdj,count(num_comp) as cant_rxh_destino1
	FROM BDDWESTG.tmp093168_detcantrxhefv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
DROP TABLE BDDWESTG.tmp093168_kpi02_cndestino2	;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpi02_cndestino2 AS
(
	SELECT ind_presdj,count(num_comp) as cant_rxh_destino2
	FROM BDDWESTG.tmp093168_detcantrxhemdb
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;



/********************INSERT EN TABLA FINAL***********************************/

  DELETE FROM BDDWESTG.T11908DETKPITRIBINT WHERE COD_KPI='K001012022'  AND FEC_CARGA=CURRENT_DATE;
  

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT 2022,z.ind_presdj,
	       'K001012022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT --x0.num_ruc,
			       x0.ind_presdj,
			       case when x0.ind_presdj=0 then (select coalesce(sum(cant_rxh_origen),0) from BDDWESTG.tmp093168_kpi01_cnorigen) else 0 end as cant_origen,
			       coalesce(x1.cant_rxh_destino1,0) as cant_destino
			FROM (
				select y.ind_presdj,SUM(y.cant_rxh_origen) as cant_rxh_origen
				from
				(
					select * from BDDWESTG.tmp093168_kpi01_cnorigen
					union all select 1,0 from (select '1' agr1) a
					union all select 0,0 from (select '0' agr0) b
				) y group by 1

			) x0
			LEFT JOIN BDDWESTG.tmp093168_kpi01_cndestino1 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;


DELETE FROM BDDWESTG.T11908DETKPITRIBINT WHERE COD_KPI='K001022022'  AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  2022,z.ind_presdj,
	        'K001022022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT 
			       x0.ind_presdj,
			       x0.cant_rxh_destino1 AS cant_origen,
			       case when x0.ind_presdj=0  then (select coalesce(sum(cant_rxh_destino2),0) from BDDWESTG.tmp093168_kpi02_cndestino2) else 0 end AS cant_destino
			FROM (
				select y.ind_presdj,SUM(y.cant_rxh_destino1) as cant_rxh_destino1
				from
				(
					select * from BDDWESTG.tmp093168_kpi01_cndestino1
					union all select 1,0 from (select '1' agr1) a
					union all select 0,0 from (select '0' agr0) b
				) y group by 1
			) x0
			LEFT JOIN BDDWESTG.tmp093168_kpi02_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

/*=============================================================================*/
/***********************Genera Detalle de Diferencias**************************/
/*=============================================================================*/	
	
	DROP TABLE VBDDWESTG.DIF_K001012022;
	CREATE MULTISET TABLE BDDWESTG.DIF_K001012022 
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
		FROM BDDWESTG.tmp093168_detcantrxhetr
		EXCEPT ALL
		SELECT num_ruc,num_serie,tip_comp,cast(num_comp as integer)
		FROM BDDWESTG.tmp093168_detcantrxhefv
	) y0
	) WITH DATA NO PRIMARY INDEX;


	DROP TABLE BDDWESTG.DIF_K001022022
	CREATE MULTISET TABLE BDDWESTG.DIF_K001022022
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
		FROM BDDWESTG.tmp093168_detcantrxhefv
		EXCEPT ALL
		SELECT num_ruc,num_serie,cod_tipcomp,cast(num_comp as integer) 
		FROM BDDWESTG.tmp093168_detcantrxhemdb
	) y0
	) WITH DATA NO PRIMARY INDEX;


/********************************************************************************/
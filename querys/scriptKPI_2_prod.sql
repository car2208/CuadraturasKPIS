/*
DROP TABLE BDDWESTG.tmp093168_udjkpigr2;
DROP TABLE BDDWESTG.tmp093168_kpigr2_periodos_ctaind;
DROP TABLE BDDWESTG.tmp093168_kpigr2_periodos_compag;
DROP TABLE BDDWESTG.tmp093168_kpigr02_detcnt_tr;
DROP TABLE BDDWESTG.tmp093168_kpigr02_detcntpertr;
DROP TABLE BDDWESTG.tmp093168_kpigr02_detcntperfv;
DROP TABLE BDDWESTG.tmp093168_kpigr02_detcntpermdb;
DROP TABLE BDDWESTG.tmp093168_kpigr02_cnorigen;
DROP TABLE BDDWESTG.tmp093168_kpigr02_cndestino1;
DROP TABLE BDDWESTG.tmp093168_kpigr02_cndestino2;
DROP TABLE BDDWESTG.DIF_K002012022
DROP TABLE BDDWESTG.DIF_K002022022
*/

/************Obtiene úlima DJ ***********************************************/

--DROP TABLE BDDWESTG.tmp093168_udjkpigr2;

CREATE MULTISET TABLE BDDWESTG.tmp093168_udjkpigr2 as
(
SELECT t2.t03nabono,t2.t03norden,t2.t03formulario,
	   t2.t03lltt_ruc,t2.t03periodo,t2.t03f_presenta 
FROM 
(
SELECT t03periodo,
	   t03lltt_ruc,
	   t03formulario,
       MAX(t03f_presenta) as t03f_presenta,
       MAX(t03nresumen) as t03nresumen,
       MAX(t03norden)  as t03norden 
FROM BDDWESTG.t03djcab
WHERE t03formulario = '0601' 
AND SUBSTR(t03periodo,1,4)='2022'
AND t03f_presenta <=DATE '2023-02-24'
GROUP BY 1,2,3
) AS t1 
INNER JOIN BDDWESTG.t03djcab t2 ON t2.t03periodo = t1.t03periodo 
AND t2.t03lltt_ruc = t1.t03lltt_ruc
AND t2.t03formulario = t1.t03formulario
AND t2.t03f_presenta = t1.t03f_presenta
AND t2.t03nresumen = t1.t03nresumen
AND t2.t03norden = t1.t03norden
)
WITH DATA NO PRIMARY INDEX;


/******************Cantidad de periodos declarados en el PLAME***************************/

--DROP TABLE BDDWESTG.tmp093168_kpigr2_periodos_ctaind;

CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr2_periodos_ctaind as
(
SELECT  DISTINCT 
        x2.dds_numruc as num_docide_aseg,
        x0.num_docide_empl,
        x0.num_paquete as num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM BDDWESTG.t727nctaind x0
INNER JOIN  BDDWESTG.tmp093168_udjkpigr2 x1 
ON  x1.t03lltt_ruc = x0.num_docide_empl
AND x1.t03nabono = x0.num_paquete
AND x1.t03formulario = x0.cod_formul 
AND x1.t03norden = x0.num_orden
INNER JOIN  BDDWESTG.dds x2
ON x0.num_docide_aseg=x2.dds_nrodoc AND cast(cast(x0.tip_docide_aseg as int) as varchar(3))=x2.dds_docide
WHERE x0.per_aporta BETWEEN '202201'AND '202212'
AND x0.cod_formul = '0601'
AND x0.cod_tributo = '030402'
AND x0.tip_trabajador = '67'
AND x0.mto_base_imp IS NOT NULL
AND x0.mto_aporta IS NOT NULL
)
WITH DATA NO PRIMARY INDEX;


/*********************************************************************************************/
---------Cantidad de periodos declarados en el PLAME otros Ingresos---------------------------


--DROP TABLE BDDWESTG.tmp093168_kpigr2_periodos_compag;

CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr2_periodos_compag AS
(

SELECT DISTINCT
	CASE WHEN x0.cod_tip_doc_ide = '06' 
	     THEN x2.ddp_numruc
	ELSE  x3.dds_numruc END AS num_rucs,
    x0.num_ruc,
    x0.num_paq as num_nabono,
    x0.formulario as cod_formul,
    x0.norden as num_orden,
    x0.per_decla
FROM BDDWESTG.t4583com_pag x0 
INNER JOIN BDDWESTG.tmp093168_udjkpigr2 x1 
ON  x1.t03lltt_ruc = x0.num_ruc
AND x1.t03nabono = x0.num_paq
AND x1.t03formulario = x0.formulario 
AND x1.t03norden = x0.norden
LEFT JOIN BDDWESTG.ddp x2 
ON x0.num_doc_ide=x2.ddp_numruc AND x0.cod_tip_doc_ide='06'
LEFT JOIN BDDWESTG.dds x3 
ON x0.num_doc_ide=x3.dds_nrodoc AND cast(cast(x0.cod_tip_doc_ide AS int) as varchar(3))=x3.dds_docide
WHERE x0.per_decla BETWEEN '202201' AND '202212'
AND x0.formulario = '0601'
AND x0.ind_com_pag = 'O'
AND x0.mto_servicio IS NOT NULL
AND  num_rucs IS NOT NULL
) WITH DATA NO PRIMARY INDEX;
;
   

--DROP TABLE BDDWESTG.tmp093168_kpigr02_detcnt_tr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_detcnt_tr AS
(
   SELECT
        TRIM(num_docide_aseg) as num_ruc,
        TRIM(num_docide_empl) AS num_docide_empl,
        num_nabono,
        cod_formul,
        num_orden,
        per_aporta
   FROM BDDWESTG.tmp093168_kpigr2_periodos_ctaind
   UNION
   SELECT TRIM(num_rucs),
    	  TRIM(num_ruc),
    	  num_nabono,
    	  cod_formul,
    	  num_orden,
    	  per_decla
   FROM BDDWESTG.tmp093168_kpigr2_periodos_compag
   WHERE TRIM(num_rucs) IN (SELECT TRIM(num_docide_aseg) FROM BDDWESTG.tmp093168_kpigr2_periodos_ctaind)
)
WITH DATA NO PRIMARY INDEX;


/*======================================================================================*/
/****************Genera Detalles con Indicador de Presentación***************************/

-------1. Detalle de Periodos  en transaccional

--DROP TABLE BDDWESTG.tmp093168_kpigr02_detcntpertr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_detcntpertr
AS(
SELECT 
		DISTINCT x0.num_ruc,
		COALESCE(x1.ind_presdj,0) as ind_presdj,
        x0.num_docide_empl,
        x0.num_nabono,
        x0.cod_formul,
        x0.num_orden,
        x0.per_aporta
FROM BDDWESTG.tmp093168_kpigr02_detcnt_tr x0
LEFT JOIN BDDWESTG.tmp093168_kpiperindj x1 on x0.num_ruc=x1.num_ruc
WHERE substr(x0.num_ruc,1,1) <>'2' 
or  x0.num_ruc in (select num_ruc from BDDWESTG.tmp093168_rucs20_incluir)
) WITH DATA NO PRIMARY INDEX ; 

-------2. Detalle de Periodos en Archivo Personalizado Fvirtual


--DROP TABLE BDDWESTG.tmp093168_kpigr02_detcntperfv;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_detcntperfv
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_doc,
					x0.periodo
	FROM BDDWESTG.t5373cas107_2802 x0
	INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.tip_comp = ' '
	AND x0.num_serie = ' '
	AND x0.num_comp = ' '
) WITH DATA NO PRIMARY INDEX ; 


-------3. Detalle de Periodos en Archivo Personalizado MongoDB

--DROP TABLE BDDWESTG.tmp093168_kpigr02_detcntpermdb;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_detcntpermdb
AS(
	SELECT DISTINCT x1.num_ruc,COALESCE(x1.ind_presdj,0) as ind_presdj,
					x0.num_doc, x0.num_perservicio
	FROM BDDWESTG.T5373CAS107_MONGODB x0
	INNER JOIN BDDWESTG.tmp093168_kpiperindj x1 ON x0.num_sec = x1.num_sec
	WHERE x0.COD_TIPCOMP = ' '
	AND x0.num_serie = ' '
	AND x0.num_comp = ' '
) WITH DATA NO PRIMARY INDEX ; 

/*======================================================================================*/
/************************Conteo para insercion en DETKPI ********************************/

---------1. Conteo transaccional

--DROP TABLE BDDWESTG.tmp093168_kpigr02_cnorigen;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_cnorigen AS
(
	SELECT ind_presdj,count(per_aporta) as cant_per_origen
	FROM BDDWESTG.tmp093168_kpigr02_detcntpertr
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

---------2. Conteo en FVirtual
--DROP TABLE BDDWESTG.tmp093168_kpigr02_cndestino1;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_cndestino1 AS
(
	SELECT ind_presdj,count(periodo) as cant_per_destino1
	FROM BDDWESTG.tmp093168_kpigr02_detcntperfv
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;

--------3 Conteo en MongoDB
--DROP TABLE BDDWESTG.tmp093168_kpigr02_cndestino2	;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_cndestino2 AS
(
	SELECT ind_presdj,count(num_perservicio) as cant_per_destino2
	FROM BDDWESTG.tmp093168_kpigr02_detcntpermdb
	GROUP BY 1
) WITH DATA NO PRIMARY INDEX;



/********************INSERT EN TABLA FINAL***********************************/

	DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
	WHERE COD_KPI='K002012022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  2022,
	        z.ind_presdj,
	       'K002012022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT
			       x0.ind_presdj,
			      case when x0.ind_presdj=0 then (select coalesce(sum(cant_per_origen),0) from BDDWESTG.tmp093168_kpigr02_cnorigen) else 0 end as cant_origen,
			       coalesce(x1.cant_per_destino1,0) as cant_destino
			FROM (
				select y.ind_presdj,SUM(y.cant_per_origen) as cant_per_origen
				from
				(
					select * from BDDWESTG.tmp093168_kpigr02_cnorigen
					union all select 1,0 from (select '1' agr1) a
					union all select 0,0 from (select '0' agr0) b
				) y group by 1
			) x0
			LEFT JOIN BDDWESTG.tmp093168_kpigr02_cndestino1 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;


	DELETE FROM BDDWESTG.T11908DETKPITRIBINT 
	WHERE COD_KPI='K002022022' AND FEC_CARGA=CURRENT_DATE;

	INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
	(COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
	SELECT  2022,
	        z.ind_presdj,
	        'K002022022',
	        CURRENT_DATE,
	        SUM(z.cant_origen),
	        SUM(z.cant_destino)
	FROM
		(
			SELECT x0.ind_presdj,
			       x0.cant_per_destino1 AS cant_origen,
			      case when x0.ind_presdj=0  then (select coalesce(sum(cant_per_destino2),0) from BDDWESTG.tmp093168_kpigr02_cndestino2) else 0 end AS cant_destino
			FROM 
			(
				select y.ind_presdj,SUM(y.cant_per_destino1) as cant_per_destino1
				from
				(
					select * from BDDWESTG.tmp093168_kpigr02_cndestino1
					union all select 1,0 from (select '1' agr1) a
					union all select 0,0 from (select '0' agr0) b
				) y group by 1
			) x0
			LEFT JOIN BDDWESTG.tmp093168_kpigr02_cndestino2 x1 
			ON x0.ind_presdj=x1.ind_presdj
		) z
	GROUP BY 1,2,3,4
	;

/******************************************************************************/

	DROP TABLE BDDWESTG.DIF_K002012022;
	CREATE MULTISET TABLE BDDWESTG.DIF_K002012022
	AS
	(

		SELECT 
		DISTINCT  y0.num_ruc as num_ruc_trab,
        y0.num_docide_empl as num_ruc_empl,
        y0.num_nabono,
        y0.cod_formul,
        y0.num_orden,
        y0.per_aporta as per_dif
	FROM BDDWESTG.tmp093168_kpigr02_detcntpertr y0
	INNER JOIN
	(
		SELECT DISTINCT num_ruc,num_docide_empl,
		                SUBSTR(per_aporta,5,2)||SUBSTR(per_aporta,1,4) as per_aporta
		FROM BDDWESTG.tmp093168_kpigr02_detcntpertr
		EXCEPT ALL
		SELECT num_ruc,num_doc,periodo 
		FROM BDDWESTG.tmp093168_kpigr02_detcntperfv
	) y1 
	ON y0.num_ruc=y1.num_ruc 
	AND y0.num_docide_empl=y1.num_docide_empl
	AND SUBSTR(y0.per_aporta,5,2)||SUBSTR(y0.per_aporta,1,4)=y1.per_aporta
	) WITH DATA NO PRIMARY INDEX;


	DROP TABLE BDDWESTG.DIF_K002022022;
	CREATE MULTISET TABLE BDDWESTG.DIF_K002022022
	AS
	(
	SELECT   DISTINCT 
			   y0.num_ruc as num_ruc_trab,
		      y0.num_doc as num_ruc_empl,
			  y0.periodo  as per_dif
	FROM
	(
		SELECT num_ruc,num_doc,periodo 
		FROM BDDWESTG.tmp093168_kpigr02_detcntperfv
		EXCEPT ALL
		SELECT num_ruc,num_doc,num_perservicio
		FROM BDDWESTG.tmp093168_kpigr02_detcntpermdb
	) y0
	)WITH DATA NO PRIMARY INDEX;

/*********************************************************************************/
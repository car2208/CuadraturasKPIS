--10255953015|E001|1986|10/06/2016|2016-10-06 10:48:23|
--10239239370|E001|450|07/24/2018|2018-07-24 12:36:08|
--10180848482|E001|3569|11/27/2020|2020-11-27 16:59:35|
--10296745192|E001|957|10/14/2021|2021-10-14 14:40:57|


SELECT * FROM BDDWESTG.t3639recibo
WHERE num_ruc in (
'10255953015'
)
--and EXTRACT(YEAR FROM fec_emision_rec) = 2022
--	AND FEC_EMISION_REC <= DATE '2022-10-27'
	AND ind_estado_rec = '0'
	AND cod_tipcomp    = '01'
	AND num_comprob    = 1986
;

SELECT DISTINCT *
FROM BDDWESTGD.t1209f616rddet x0
INNER JOIN BDDWESTGD.tmp093168_udjkpi1 x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '202201' and '202212'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '02'
AND substr(x0.num_serie_cp,1,1) ='E'
AND x0.num_docide_dec='10255953015'
AND cast(x0.num_cp as int)=1986
;



/************************************************************/

SELECT * FROM BDDWESTG.t3639recibo
WHERE num_ruc in (
'10239239370'
)
--and EXTRACT(YEAR FROM fec_emision_rec) = 2022
--	AND FEC_EMISION_REC <= DATE '2022-10-27'
	AND ind_estado_rec = '0'
	AND cod_tipcomp    = '01'
	AND num_comprob    = 450
;

SELECT DISTINCT *
FROM BDDWESTG.t1209f616rddet x0
INNER JOIN BDDWESTGD.tmp093168_udjkpi1 x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '202201' and '202212'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '02'
AND substr(x0.num_serie_cp,1,1) ='E'
AND x0.num_docide_dec='10239239370'
AND cast(x0.num_cp as int)=450
;


/**************************************************************/
SELECT * FROM BDDWESTG.t3639recibo
WHERE num_ruc in (
'10296745192'
)
and EXTRACT(YEAR FROM fec_emision_rec) = 2022
	AND FEC_EMISION_REC <= DATE '2022-10-27'
	AND ind_estado_rec = '0'
	AND cod_tipcomp = '01'
	AND num_comprob=957


SELECT DISTINCT *
FROM BDDWESTG.t1209f616rddet x0
INNER JOIN BDDWESTGD.tmp093168_udjkpi1 x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '202201' and '202212'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '02'
AND substr(x0.num_serie_cp,1,1) ='E'
AND x0.num_docide_dec='10296745192'
AND cast(x0.num_cp as int)=957

/****************************************************************/

SELECT * FROM BDDWESTG.t3639recibo
WHERE num_ruc in (
'10180848482'
)
and EXTRACT(YEAR FROM fec_emision_rec) = 2022
	AND FEC_EMISION_REC <= DATE '2022-10-27'
	AND ind_estado_rec = '0'
	AND cod_tipcomp = '01'
	AND num_comprob=3569


SELECT DISTINCT *
FROM BDDWESTG.t1209f616rddet x0
INNER JOIN BDDWESTGD.tmp093168_udjkpi1 x1 ON 
     x0.num_paq = x1.t03nabono
AND x0.formulario = x1.t03formulario
AND x0.norden = x1.t03norden
AND x0.per_periodo = x1.t03periodo
WHERE x1.t03periodo BETWEEN '202201' and '202212'
AND x1.t03formulario = '0616'
AND x0.tip_docide_dec = '6'
AND x0.tip_cp = '02'
AND substr(x0.num_serie_cp,1,1) ='E'
AND x0.num_docide_dec='10180848482'
AND cast(x0.num_cp as int)=3569

/***************************************************************/
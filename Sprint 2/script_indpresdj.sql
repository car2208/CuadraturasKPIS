
/*========================================================================================= */
/**********************************ARCHIVO PERSONALIZADO************************************/
/*========================================================================================= */

/**********Determina Indicador de presentación de DJ Anual *********************************/


CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj1 as
(
SELECT num_ruc,MAX(num_sec) as num_sec
FROM bddwestgd.t5847ctldecl 
WHERE num_ejercicio = 2022
AND num_formul = '0709' 
AND ind_actual = '1' 
AND ind_estado = '0' 
AND ind_proceso = '1'
GROUP BY 1
) with data no primary INDEX ON COMMIT PRESERVE ROWS
;

------------1. Sí presentaron ----------------------

CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj2 as
(
SELECT num_ruc,MAX(num_sec) as num_sec 
FROM bddwestgd.t5847ctldecl
WHERE num_ejercicio = 2022
AND num_formul = '0709' 
AND ind_estado = '2'
GROUP BY 1
)  with data no primary INDEX ON COMMIT PRESERVE ROWS
;

------------2. No presentaron-------------------------


CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_sindj as (
SELECT num_ruc, num_sec FROM tmp093168_kpiperson_dj1 
WHERE num_ruc NOT IN ( SELECT num_ruc FROM tmp093168_kpiperson_dj2)
)  WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS
;

------------3. Consolida Indicador -------------------


DROP TABLE bddwestgd.tmp093168_kpiperindj;
CREATE MULTISET TABLE bddwestgd.tmp093168_kpiperindj as (
SELECT num_ruc, num_sec,0 as ind_presdj FROM tmp093168_kpiperson_sindj 
UNION ALL
SELECT num_ruc,num_sec,1 FROM tmp093168_kpiperson_dj2
)
 WITH DATA PRIMARY INDEX (num_sec)
;

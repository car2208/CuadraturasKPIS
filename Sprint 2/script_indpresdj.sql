


DROP TABLE  bddwestg.tmp093168_rucs20_incluir;
CREATE MULTISET TABLE  bddwestg.tmp093168_rucs20_incluir
(
num_ruc varchar(11)
) UNIQUE PRIMARY INDEX(num_ruc)
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20103702489');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20106319805');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20136024681');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20147988461');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20148016691');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20148029246');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20154478770');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20162559479');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20168920092');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20170616074');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20175365673');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20175986350');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20185359477');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20187935122');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20196538381');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20197169291');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20201745871');
INSERT INTO  bddwestg.tmp093168_rucs20_incluir VALUES('20527243093');


/*========================================================================================= */
/**********************************ARCHIVO PERSONALIZADO************************************/
/*========================================================================================= */

/**********Determina Indicador de presentación de DJ Anual *********************************/


CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj1 as
(
SELECT num_ruc,MAX(num_sec) as num_sec
FROM bddwestg.t5847ctldecl 
WHERE num_ejercicio = 2022
AND num_formul = '0709' 
AND ind_actual = '1' 
AND ind_estado = '0' 
AND ind_proceso = '1'
AND cast(fec_creacion as date) <= CAST('2023-02-08' AS DATE FORMAT 'YYYY-MM-DD')
GROUP BY 1
) with data no primary INDEX ON COMMIT PRESERVE ROWS
;

------------1. Sí presentaron ----------------------

CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj2 as
(
SELECT num_ruc,MAX(num_sec) as num_sec 
FROM bddwestg.t5847ctldecl
WHERE num_ejercicio = 2022
AND num_formul = '0709' 
AND ind_estado = '2'
AND cast(fec_creacion as date) <= CAST('2023-02-08' AS DATE FORMAT 'YYYY-MM-DD')
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


DROP TABLE bddwestg.tmp093168_kpiperindj;
CREATE MULTISET TABLE bddwestg.tmp093168_kpiperindj as (
SELECT uni.num_ruc,uni.num_sec,uni.ind_presdj 
FROM 
    (
       SELECT num_ruc, num_sec,0 as ind_presdj FROM tmp093168_kpiperson_sindj 
       UNION ALL
       SELECT num_ruc,num_sec,1 FROM tmp093168_kpiperson_dj2
    ) uni
    WHERE SUBSTR(uni.num_ruc,1,1)<>'2' 
    OR uni.num_ruc IN (SELECT num_ruc FROM bddwestg.tmp093168_rucs20_incluir)
)
 WITH DATA PRIMARY INDEX (num_sec)
;

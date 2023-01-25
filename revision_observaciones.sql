

select * from BDDWESTG.DIF_K002012022 where num_ruc='10062568009'
or num_ruc='06256800'


select * FROM BDDWESTG.tmp093168_kpigr02_detcntpertr where num_ruc in ('10062568009','06256800')

SELECT * FROM BDDWESTG.tmp093168_kpigr02_detcntperfv where num_ruc in ('10062568009','06256800')

SELECT DISTINCT num_ruc,ind_presdj,num_docide_empl,
		                SUBSTR(per_aporta,5,2)||SUBSTR(per_aporta,1,4) as per_aporta
FROM BDDWESTG.tmp093168_kpigr02_detcntpertr WHERE num_ruc in ('10062568009','06256800')




CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr2_periodos_ctaind2
AS
(
select * from BDDWESTG.tmp093168_kpigr2_periodos_ctaind
) WITH DATA NO PRIMARY INDEX;

DROP TABLE BDDWESTG.tmp093168_kpigr2_periodos_ctaind
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


CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_detcnt_tr_2
AS (
SELECT * FROM BDDWESTG.tmp093168_kpigr02_detcnt_tr
) WITH DATA NO PRIMARY INDEX;

DROP TABLE BDDWESTG.tmp093168_kpigr02_detcnt_tr;
CREATE MULTISET TABLE BDDWESTG.tmp093168_kpigr02_detcnt_tr AS
(
   SELECT
        num_docide_aseg as num_ruc,
        num_docide_empl,
        num_nabono,
        cod_formul,
        num_orden,
        per_aporta
   FROM BDDWESTG.tmp093168_kpigr2_periodos_ctaind
   UNION ALL
   SELECT num_rucs,
    	  num_ruc,
    	  num_nabono,
    	  cod_formul,
    	  num_orden,
    	  per_decla
   FROM BDDWESTG.tmp093168_kpigr2_periodos_compag
)
WITH DATA NO PRIMARY INDEX;


SELECT COUNT(*) FROM BDDWESTG.tmp093168_kpigr02_detcnt_tr_2
SELECT COUNT(*) FROM BDDWESTG.tmp093168_kpigr02_detcnt_tr;


SELECT  COUNT(*)
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

SELECT * FROM 10448102799
44810279

select LENGTH(TRIM(NUM_RUC)) AS LEN,COUNT(*) AS CANT 
FROM BDDWESTG.tmp093168_kpigr02_detcntpertr
GROUP BY 1


select LENGTH(TRIM(num_docide_aseg)) AS LEN,COUNT(*) AS CANT 
FROM BDDWESTG.tmp093168_kpigr2_periodos_ctaind
GROUP BY 1


select LENGTH(TRIM(NUM_RUCS)) AS LEN,COUNT(*) AS CANT 
FROM BDDWESTG.tmp093168_kpigr2_periodos_compag
GROUP BY 1


select LENGTH(TRIM(NUM_RUCS)) AS LEN,COUNT(*) AS CANT 
FROM BDDWESTG.tmp093168_kpigr02_detcnt_tr
GROUP BY 1

select * from BDDWESTG.DIF_K002012022 where num_ruc='10062568009'
or num_ruc='06256800'
union 
select * from BDDWESTG.DIF_K002012022 where num_ruc='10448102799'
or num_ruc='44810279'
union 
select * from BDDWESTG.DIF_K002012022 where num_ruc='10438758301'
or num_ruc='43875830'
union 
select * from BDDWESTG.DIF_K002012022 sample 2000


10438758301
43875830



select * from BDDWESTG.DIF_K002012022 where num_ruc='10062568009'
or num_ruc='06256800'
union
select * from BDDWESTG.DIF_K002012022 where num_ruc='10448102799'
or num_ruc='44810279'
union
select * from BDDWESTG.DIF_K002012022 where num_ruc='10438758301'
or num_ruc='43875830'
union
select top 2000 * from BDDWESTG.DIF_K002012022





10028654311

02865431

DIFIERE 2 PERIODOS RESPECTO AL ORIGEN, DEBIDO A LA FECHA DE PROCESAMIENTO
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA
15504922909

18030010

DIFIERE 2 PERIODOS RESPECTO AL ORIGEN, DEBIDO A LA DISPONIBILIDAD DE LA CARGA DE LA ANEXA
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA
10469687541

46968754

DIFIERE 2 PERIODOS RESPECTO AL ORIGEN, DEBIDO A LA DISPONIBILIDAD DE LA CARGA DE LA ANEXA
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA
10092097833

09209783

DIFIERE 2 PERIODOS RESPECTO AL ORIGEN, DEBIDO A LA DISPONIBILIDAD DE LA CARGA DE LA ANEXA
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA


SELECT * FROM BDDWESTG.tmp093168_kpigr04_detcntpertr
WHERE num_ruc IN 
('10028654311','15504922909','10469687541','10092097833'
'02865431','18030010','46968754','09209783'
)




10091777385

20297939131

DIFIERE DOS PERIODOS RESPECTO AL ORIGEN, DEBIDO A LA DISPONIBILIDAD DE LA CARGA DE LA ANEXA
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA
10006762919

20519610214

DIFIERE 1 PERIODO RESPECTO AL ORIGEN, DEBIDO A LA FECHA DE PROCESAMIENTO
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA
10099923500

20484347205

DIFIERE 1 PERIODO RESPECTO AL ORIGEN, DEBIDO A LA DISPONIBILIDAD DE LA CARGA DE LA ANEXA
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA
10178855439

20132113611

DIFIERE 1 PERIODO RESPECTO AL ORIGEN, DEBIDO A LA DISPONIBILIDAD DE LA CARGA DE LA ANEXA
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA
10430389632

20479569861

DIFIERE 2 PERIODOS RESPECTO AL ORIGEN, DEBIDO A LA FECHA DE PROCESAMIENTO Y A LA DISPONIBILIDAD DE LA CARGA DE LA ANEXA
PERIODO REPORTADO NO CORRESPONDE A LA DIFERENCIA



SELECT * FROM BDDWESTG.DIF_K003012022 
WHERE NUM_RUC IN
 (
'10091777385',
'10006762919',
'10099923500',
'10178855439',
'10430389632',
  '09177738',
  '00676291',
  '09992350',
  '17885543',
  '43038963'
 )

SELECT * FROM BDDWESTG.DIF_K003012022 
WHERE NUM_RUC IN
 ('10091777385') AND num_docide_empl='20297939131'



SELECT * FROM BDDWESTG.tmp093168_kpigr03_detcntperfv WHERE NUM_RUC='10091777385'
AND NUM_DOC='20297939131'

SELECT * FROM BDDWESTG.tmp093168_kpigr03_detcntpermdb WHERE NUM_RUC='10091777385'



      SELECT DISTINCT TRIM(num_ruc) as num_ruc,ind_presdj,TRIM(num_docide_empl) as num_docide_empl,
                      SUBSTR(per_decla,5,2)||SUBSTR(per_decla,1,4) as per_decla
      FROM BDDWESTG.tmp093168_kpigr03_detcntpertr
      WHERE NUM_RUC='10091777385'
        AND num_docide_empl='20297939131'
      EXCEPT ALL
      SELECT TRIM(num_ruc),ind_presdj,TRIM(num_doc),TRIM(periodo) 
      FROM BDDWESTG.tmp093168_kpigr03_detcntperfv
      where NUM_RUC='10091777385'
       AND NUM_DOC='20297939131'


SELECT TRIM(num_ruc),ind_presdj,TRIM(num_doc),TRIM(periodo) 
      FROM BDDWESTG.tmp093168_kpigr03_detcntpermdb
      where NUM_RUC='10091777385'
       AND NUM_DOC='20297939131'




select * from BDDWESTG.DIF_K004012022
 where num_ruc in ('10028654311','02865431')


 select * from BDDWESTG.tmp093168_kpigr04_detcntperfv
 where num_ruc in  ('10028654311','02865431')



 SELECT * FROM BDDWESTG.T11908DETKPITRIBINT WHERE FEC_CARGA>= DATE '2022-11-30'
 AND COD_KPI IN (
 	'K001012022',
'K001022022',
'K002012022',
'K002022022',
'K003012022',
'K003022022',
'K004012022',
'K004022022',
'K005012022',
'K005022022'
 	)





 INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
         'K004012022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT
             x0.ind_presdj,
             x0.cant_per_origen as cant_origen,
             coalesce(x1.cant_per_destino1,0) as cant_destino
      FROM BDDWESTG.tmp093168_kpigr04_cnorigen x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr04_cndestino1 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;


  INSERT INTO BDDWESTG.T11908DETKPITRIBINT 
  (COD_PER,IND_PRESDJ,COD_KPI,FEC_CARGA,CNT_REGORIGEN,CNT_REGIDESTINO)
  SELECT  '2022',
          z.ind_presdj,
          'K004022022',
          CURRENT_DATE,
          SUM(z.cant_origen),
          SUM(z.cant_destino)
  FROM
    (
      SELECT x0.ind_presdj,
             x0.cant_per_destino1 AS cant_origen,
             coalesce(x1.cant_per_destino2,0) AS cant_destino
      FROM BDDWESTG.tmp093168_kpigr04_cndestino1 x0
      LEFT JOIN BDDWESTG.tmp093168_kpigr04_cndestino2 x1 
      ON x0.ind_presdj=x1.ind_presdj
    ) z
  GROUP BY 1,2,3,4
  ;
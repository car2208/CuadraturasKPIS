/***********************************************************************************************/
--Cantidad Total de Contribuyentes

SELECT COUNT(*) REG FROM ddp                   --15,391,232

--Cantidad Total de Contribuyente con CIIU v4 actualizado en su Actividad Económica Principal

SELECT  COUNT(*) REG FROM t5667acteco
WHERE cod_tipact = 'P'            --8,910,092

/************************************************************************************************/

--------Cantidad Total de Contribuyentes Activos de Renta de 3ra excepto NRUS-----------------------

SELECT DISTINCT num_ruc 
FROM vfp
WHERE vfp_codtri in ('030301','033101','035101','034101','036101','031101','031201')
INSERT INTO tempvfp3ra_01 with NO log;

SELECT * FROM ddp
WHERE ruc in(tempvfp3ra_01) 
AND estado='00'
INSERT INTO tempvfp3ra_02 with NO log;

SELECT count(*) reg FROM tempvfp3ra_02

---Cantidad Total de Contribuyentes Activos de Renta de 3ra excepto NRUS con CIIU v4 actualizado en su Actividad Económica Principal

SELECT count(*) FROM t5667actecto
WHERE num_ruc in (SELECT num_ruc FROM tempvfp3ra_02)
AND cod_tipact ='P'

/*************************************************************************************************/

---Cantidad Total de Contribuyentes 

    SELECT DISTINCT  num_docidenti , count(*) reg
    FROM sif:t2017identif
    INSERT INTO tempCIC01 with NO log;

    SELECT count(*) reg FROM tempCIC01

--Cantidad Total de Contribuyentes que cuentan con solamente un CIC registrado

    SELECT nombre, cic, count(*) cant 
    FROM sif:t2017identif
    GROUP BY nombre, cic
    HAVING count(*)=1
    INSERT INTO tempCIC02 with NO log;

    SELECT count(*) reg FROM tempCIC02

/***************************************************************************************************/
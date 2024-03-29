#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Base de datos Teradata - Staging
### $6 : Ruta Log TERADATA
### $7 : Periodo :2022
### $8 : Fecha de corte

##sh /work1/teradata/shells/093168/J093168_CalculateIndPresDJ.sh tdsunat usr_carga_prod twusr_carga_prod BDDWEDQ BDDWESTG /work1/teradata/log/093168 2022 2023-02-08
##sh /work1/teradata/shells/093168/J093168_CalculateIndPresDJ.sh tdtp01s2 usr_carga_desa twusr_carga_desa BDDWEDQD BDDWESTGD /work1/teradata/log/093168 2022 2023-02-08
################################################################################


if [ $# -ne 8 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
BD_STG=${5}
path_log_TD=${6}
PERIODO=${7}
FECHA_CORTE=${8}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'

rm -f ${FILE_KPI01}
rm -f ${FILE_KPI02}

bteq <<EOF>${FILELOG} 2>${FILEERR}

LOGON ${LOGONDB};

DATABASE ${BD_DQ};

.SET FORMAT OFF;
.SET WIDTH 32000;
.SET SEPARATOR '|';
.SET TITLEDASHES OFF;

SEL CURRENT_TIMESTAMP;

DROP TABLE  ${BD_STG}.tmp093168_rucs20_incluir;
CREATE MULTISET TABLE  ${BD_STG}.tmp093168_rucs20_incluir
(
num_ruc varchar(11)
) UNIQUE PRIMARY INDEX(num_ruc)
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20103702489');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20106319805');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20136024681');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20147988461');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20148016691');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20148029246');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20154478770');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20162559479');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20168920092');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20170616074');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20175365673');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20175986350');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20185359477');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20187935122');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20196538381');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20197169291');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20201745871');
INSERT INTO  ${BD_STG}.tmp093168_rucs20_incluir VALUES('20527243093');



/*========================================================================================= */
/**********************************ARCHIVO PERSONALIZADO************************************/
/*========================================================================================= */

/**********Determina Indicador de presentación de DJ Anual *********************************/


CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj1 as
(
SELECT num_ruc,MAX(num_sec) as num_sec
FROM ${BD_STG}.t5847ctldecl 
WHERE num_ejercicio = ${PERIODO}
AND num_formul = '0709' 
AND ind_actual = '1' 
AND ind_estado = '0' 
AND ind_proceso = '1'
--AND cast(fec_creacion as date) <= CAST('${FECHA_CORTE}' AS DATE FORMAT 'YYYY-MM-DD')
GROUP BY 1
) with data no primary INDEX ON COMMIT PRESERVE ROWS
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

------------1. Sí presentaron ----------------------

CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_dj2 as
(
SELECT num_ruc,MAX(num_sec) as num_sec 
FROM ${BD_STG}.t5847ctldecl
WHERE num_ejercicio = ${PERIODO}
AND num_formul = '0709' 
AND ind_estado = '2'
--AND cast(fec_creacion as date) <= CAST('${FECHA_CORTE}' AS DATE FORMAT 'YYYY-MM-DD')
GROUP BY 1
)  with data no primary INDEX ON COMMIT PRESERVE ROWS
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

------------2. No presentaron-------------------------


CREATE MULTISET VOLATILE TABLE tmp093168_kpiperson_sindj as (
SELECT num_ruc, num_sec FROM tmp093168_kpiperson_dj1 
WHERE num_ruc NOT IN ( SELECT num_ruc FROM tmp093168_kpiperson_dj2)
)  WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


------------3. Consolida Indicador -------------------



SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_STG}' AND TableName = 'tmp093168_kpiperindj';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_STG}.tmp093168_kpiperindj;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_STG}.tmp093168_kpiperindj as (
    SELECT uni.num_ruc,uni.num_sec,uni.ind_presdj 
    FROM 
    (
    SELECT num_ruc, num_sec,0 as ind_presdj FROM tmp093168_kpiperson_sindj 
    UNION ALL
    SELECT num_ruc,num_sec,1 FROM tmp093168_kpiperson_dj2
    ) uni
    WHERE SUBSTR(uni.num_ruc,1,1)<>'2' OR uni.num_ruc IN (SELECT num_ruc FROM ${BD_STG}.tmp093168_rucs20_incluir)
)
 WITH DATA PRIMARY INDEX (num_sec)
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 


/********************************************************************************************************/


SEL CURRENT_TIMESTAMP;

    
LOGOFF;
QUIT 0;
.LABEL error_shell
LOGOFF;
QUIT 8;


EOF

CODRET=$?

FILEMSG=msg.log

if [ $CODRET -ne 0 ]; then
    echo ' '
    echo '  ***********************************************' >> ${FILEMSG}
    echo '  *** ' ${NOMBREBASE}  '  ***' >> ${FILEMSG}
    echo '  ***           ERROR!!!! EN PROCESO          ***' >> ${FILEMSG}
    echo '  ***********************************************' >> ${FILEMSG}
    cat ${FILEMSG} >> ${FILELOG}
    cat ${FILEMSG}
    echo 'Revisar la ejecucion de la shell en los siguientes archivos:'
    echo ${FILELOG}
    echo ${FILEERR}
    echo ' '
    echo '######  < Inicio >  ########################################################'
    echo ' '
    echo '     ######  I.  VER ERROR #################################################'
    cat ${FILEERR}
    echo '     ######  II. VER LOG   #################################################'
    cat ${FILELOG}
    echo '######  -  Fin -  ##########################################################'
    rm ${FILEMSG}
    exit 1
fi

if [ $CODRET = 0 ]
then
    echo ' '
    echo '  ***********************************************' >> ${FILEMSG}
    echo '  *** ' ${NOMBREBASE}  '  ***' >> ${FILEMSG}
    echo '  ***            TERMINO PROCESO OK           ***' >> ${FILEMSG}
    echo '  ***********************************************' >> ${FILEMSG}
    cat ${FILEMSG} >> ${FILELOG}
    cat ${FILEMSG}
    echo 'Revisar la ejecucion de la shell en los siguientes archivos:'
    echo ${FILELOG}
    echo ${FILEERR}
    echo ' '
    echo '######  < Inicio >  ########################################################'
    echo ' '
    echo '     ######  I.  VER ERROR #################################################'
    cat ${FILEERR}
    echo '     ######  II. VER LOG   #################################################'
    cat ${FILELOG}
    echo '######  -  Fin -  ##########################################################'
    rm ${FILEMSG}
    exit 0
fi
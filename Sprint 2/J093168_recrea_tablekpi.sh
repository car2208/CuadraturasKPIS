#### Se ejecuta desde jobs DataStage
#### ---------------------------------------------------------------------------
## Parametros: 
### $1 : Server TERADATA
### $2 : User TERADATA
### $3 : Teradata Wallet
### $4 : Base de datos Teradata - DQ
### $5 : Ruta Log


################################################################################


if [ $# -ne 5 ]; then echo 'Numero incorrecto de Parametros'; exit 1; fi


### PARAMETROS
server_TD=${1}
username_TD=${2}
walletPwd_TD=${3}
BD_DQ=${4}
path_log_TD=${5}

MY_DIR=`dirname $0`
NOMBREBASE=`basename ${0} .sh`
DATE=`date +%Y%m%d`
LOGONDB=${server_TD}'/'${username_TD}',$tdwallet('${walletPwd_TD}')'
FILELOG=${path_log_TD}'/'${NOMBREBASE}'.log'
FILEERR=${path_log_TD}'/'${NOMBREBASE}'.err'



bteq <<EOF>${FILELOG} 2>${FILEERR}

LOGON ${LOGONDB};

DATABASE ${BD_DQ};

.SET FORMAT OFF;
.SET WIDTH 32000;
.SET SEPARATOR '|';
.SET TITLEDASHES OFF;

SEL CURRENT_TIMESTAMP;

/**********************************************************************************************/

SELECT 1 FROM  dbc.TablesV WHERE databasename = '${BD_DQ}' AND TableName = 'T11908DETKPITRIBINT_NEW';
.IF activitycount = 0 THEN .GOTO ok 

DROP TABLE ${BD_DQ}.T11908DETKPITRIBINT_NEW;
.IF ERRORCODE <> 0 THEN .GOTO error_shell;

.label ok;

CREATE MULTISET TABLE ${BD_DQ}.T11908DETKPITRIBINT_NEW (

   COD_PER              VARCHAR(6)                     not null title 'Código que representa el periodo tributario',
   IND_PRESDJ           SMALLINT                       not null title 'Indicador si presentó DJ',
   COD_KPI              VARCHAR(10)                    title 'Código de KPI hijo',
   CNT_REGORIGEN        INTEGER                        title 'Conteo en el origen',
   CNT_REGIDESTINO      INTEGER                        title 'Conteo en el destino',
   MTO_REGORIGEN        DECIMAL(20, 2)                 title 'Suma de montos del origen',
   MTO_REGIDESTINO      DECIMAL(20, 2)                 title 'Suma de montos del destino',
   FEC_CARGA            DATE                           format 'YYYY-MM-DD' title 'Fecha de carga'
)
primary index PI_T11908(COD_PER,IND_PRESDJ,COD_KPI)
unique index IN01T11908(COD_PER,IND_PRESDJ,FEC_CARGA,COD_KPI);

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

INSERT INTO ${BD_DQ}.T11908DETKPITRIBINT_NEW
(cod_per,ind_presdj,cod_kpi,cnt_regorigen,cnt_regidestino,fec_carga)
select cod_per,
	   ind_presdj,
	   cod_kpi,
	   cnt_regorigen,
	   cnt_regidestino,
	   fec_carga
from ${BD_DQ}.T11908DETKPITRIBINT
;

.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

/*********************************************************************************************/

RENAME TABLE ${BD_DQ}.T11908DETKPITRIBINT TO ${BD_DQ}.T11908DETKPITRIBINT_OLD;
.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

RENAME TABLE ${BD_DQ}.T11908DETKPITRIBINT_NEW TO ${BD_DQ}.T11908DETKPITRIBINT;
.IF ERRORCODE <> 0 THEN .GOTO error_shell; 

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
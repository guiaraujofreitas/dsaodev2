import os
import logging

path = '/home/guilherme/Documentos/repos/ds_ao_dev/projeto_webscrapy/'
if not os.path.exists( path + 'Logs' ):
    os.makedirs( path + 'Logs' )

logging.basicConfig( 
    filename=path + 'Logs/hm_etl.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger( 'hm_etl' )

logger.debug( 'O valor da variavel X eh de %s', 10 )
logger.info( 'Essa eh a interacao de Numero %s', 40 )
logger.warning( 'Esse ponto do codigo precisa ser melhorado ' )

import os
import logging


path = '/home/guilherme/Documentos/repos/ds_ao_dev/projeto_webscrapy/scripts_python/'

if not os.path.exists( path+ 'Logs_teste' ):

	os.makedirs( 'Logs_teste' )
logging.basicConfig(
		   filename='Logs_teste/webscraping_hm.txt',
		   format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
		   level=logging.DEBUG )
logger = logging.getLogger( 'webscraping_hm' )


logger.debug('testando 1')
logger.info('testando 2')
logger.warning('testando 3')
logger.error('Not executed')

# Scrapy settings for eurobarometer project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'eurobarometer'
BOT_VERSION = '1.0'

SPIDER_MODULES = ['eurobarometer.spiders']
NEWSPIDER_MODULE = 'eurobarometer.spiders'
USER_AGENT = '%s/%s' % (BOT_NAME, BOT_VERSION)

ITEM_PIPELINES = [
    'eurobarometer.pipelines.EurobarometerPipeline',
]
DOWNLOAD_DELAY = 5 
DOWNLOAD_TIMEOUT = 20
CONCURRENT_REQUESTS_PER_DOMAIN = 1
HTTPCACHE_ENABLED = True
HTTPCACHE_DIR = '/var/local/scrapy'

JOBDIR = '/home/iand/wip/eurobarometer'
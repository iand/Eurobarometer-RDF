This is a scrapy (http://scrapy.org) spider that crawls Eurobarometer data and converts to RDF.

Change the HTTPCACHE_DIR and JOBDIR paths in eurobarometer/settings.py

Start the spider by running the following from the root directory of the project

scrapy crawl csvscrape

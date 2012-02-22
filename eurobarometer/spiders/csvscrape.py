from scrapy.http import Request
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule
from eurobarometer.items import EurobarometerItem
from eurobarometer.pipelines import countries
import re

class CsvscrapeSpider(CrawlSpider):
  name = 'csvscrape'
  allowed_domains = ['ec.europa.eu']
  start_urls = ['http://ec.europa.eu/public_opinion/cf/step1.cfm']

  def parse(self, response):
    for m in re.finditer('<div id="a_(\d+)" class="eqs_possible_answer">', response.body_as_unicode(), re.S):
      keyid = m.group(1)
      for id in countries:
        date_url = 'http://ec.europa.eu/public_opinion/cf/step3.cfm?keyID=%s&nationID=%s' % (keyid, id)
        yield Request(url=date_url, callback=self.parse_dates, meta={'keyid' : keyid, 'nationid':id})



  def parse_dates(self, response):
    dates = re.findall('<option value="(\d\d\d\d\.\d\d)" >\d\d\d\d\.\d\d</option>', response.body_as_unicode(), re.S)
    if len(dates):
      keyid = response.meta['keyid']
      nationid = response.meta['nationid']
      
      startdate = dates[0]
      enddate = dates[-1]
      csv_url = "http://ec.europa.eu/public_opinion/cf/exp_csv.cfm?keyID=%s&nationID=%s&startdate=%s&enddate=%s" % (keyid, nationid, startdate, enddate)
      yield Request(csv_url, callback=self.parse_csv, meta={'keyid' : keyid, 'nationid':nationid, 'startdate':startdate, 'enddate':enddate})

  def parse_csv(self, response):
    keyid = response.meta['keyid']
    nationid = response.meta['nationid']    

    if nationid == 16:
     return # not doing EU aggregate

    header_lines = []
    answers = []
    data = []

    triples = ''

    READING_HEADER = 0
    READING_DATA = 1
    READING_FOOTER = 1

    state = READING_HEADER
    for row in response.body_as_unicode().split("\r"):
     row = row.strip()
     if state == READING_HEADER:
      if len(row):
       m = re.search('^"([^"]+)"$', row)
       if m:
        header_lines.append(m.group(1))
       else:
        m = re.search('^([^\s]+)$', row)
        if m:
         header_lines.append(m.group(1))
        else:
         state = READING_DATA
         for answer in row.split(','):
           answer = answer.strip()
           if answer[0] == '"':
             answer = answer[1:-1]
           answers.append(answer)
     elif state == READING_DATA:
      if len(row) > 0:
       data.append(row.split(","))   
      else:
       break

    label = " " . join(header_lines[:-1])
    # print label
    # print answer_line
    # print data

    item = EurobarometerItem()
    item['label'] = label
    item['answers'] = answers
    item['data'] = data
    item['keyid'] = response.meta['keyid']
    item['nationid'] = response.meta['nationid']    
    yield item


       
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html
from rdfgenutils import triple, Namespace, RDF, RDFS
import re

BASE_URI = "http://data.kasabi.com/dataset/eurobarometer-standard"

QB = Namespace('http://purl.org/linked-data/cube#')
EB = Namespace('%s/schema/' % BASE_URI)

countries = {
  '11': { 'label': 'Austria', 'uri':'http://data.kasabi.com/dataset/countries/AT' },
  '1': { 'label': 'Belgium', 'uri':'http://data.kasabi.com/dataset/countries/BE' },
  '27': { 'label': 'Bulgaria', 'uri':'http://data.kasabi.com/dataset/countries/BG' },
  '28': { 'label': 'Croatia', 'uri':'http://data.kasabi.com/dataset/countries/HR' },
  '17': { 'label': 'Czech Republic', 'uri':'http://data.kasabi.com/dataset/countries/CZ' },
  '2': { 'label': 'Denmark', 'uri':'http://data.kasabi.com/dataset/countries/DK' },
  '16': { 'label': 'EU', 'uri':'' },
  '18': { 'label': 'Estonia', 'uri':'http://data.kasabi.com/dataset/countries/EE' },
  '13': { 'label': 'Finland', 'uri':'http://data.kasabi.com/dataset/countries/FI' },
  '32': { 'label': 'Former Yugoslav Republic of Macedonia', 'uri':'http://data.kasabi.com/dataset/countries/MK' },
  '6': { 'label': 'France', 'uri':'http://data.kasabi.com/dataset/countries/FR' },
  '3': { 'label': 'Germany', 'uri':'http://data.kasabi.com/dataset/countries/DE' },
  '4': { 'label': 'Greece', 'uri':'http://data.kasabi.com/dataset/countries/GR' },
  '22': { 'label': 'Hungary', 'uri':'http://data.kasabi.com/dataset/countries/HU' },
  '33': { 'label': 'Iceland', 'uri':'http://data.kasabi.com/dataset/countries/IS' },
  '7': { 'label': 'Ireland', 'uri':'http://data.kasabi.com/dataset/countries/IE' },
  '8': { 'label': 'Italy', 'uri':'http://data.kasabi.com/dataset/countries/IT' },
  '20': { 'label': 'Latvia', 'uri':'http://data.kasabi.com/dataset/countries/LV' },
  '21': { 'label': 'Lithuania', 'uri':'http://data.kasabi.com/dataset/countries/LT' },
  '9': { 'label': 'Luxembourg', 'uri':'http://data.kasabi.com/dataset/countries/LU' },
  '23': { 'label': 'Malta', 'uri':'http://data.kasabi.com/dataset/countries/MT' },
  '34': { 'label': 'Montenegro', 'uri':'http://data.kasabi.com/dataset/countries/ME' },
  '24': { 'label': 'Poland', 'uri':'http://data.kasabi.com/dataset/countries/PL' },
  '12': { 'label': 'Portugal', 'uri':'http://data.kasabi.com/dataset/countries/PT' },
  '19': { 'label': 'Republic of Cyprus', 'uri':'http://data.kasabi.com/dataset/countries/CY' },
  '29': { 'label': 'Romania', 'uri':'http://data.kasabi.com/dataset/countries/RO' },
  '26': { 'label': 'Slovakia', 'uri':'http://data.kasabi.com/dataset/countries/SK' },
  '25': { 'label': 'Slovenia', 'uri':'http://data.kasabi.com/dataset/countries/SI' },
  '5': { 'label': 'Spain', 'uri':'http://data.kasabi.com/dataset/countries/ES' },
  '14': { 'label': 'Sweden', 'uri':'http://data.kasabi.com/dataset/countries/SE' },
  '10': { 'label': 'The Netherlands', 'uri':'http://data.kasabi.com/dataset/countries/NL' },
  '30': { 'label': 'Turkey', 'uri':'http://data.kasabi.com/dataset/countries/TR' },
  '15': { 'label': 'UK', 'uri':'http://data.kasabi.com/dataset/countries/GB' },
}


class EurobarometerPipeline(object):
    def __init__(self):
        self.file = open('results.nt', 'ab')
          
    def process_item(self, item, spider):
      keyid = item['keyid']
      nationid = item['nationid']
      answers = item['answers']
      label = item['label']
      data = item['data']


      dataset_uri = "%s/dataset" % (BASE_URI)
      question_uri = "%s/question/%s" % (BASE_URI, keyid)
      triples = triple(question_uri, RDFS['label'], label)
      for v in range(1, len(answers)):
       triples += triple(EB['q%sa%s' % (keyid, v)], RDF['type'], QB['MeasureProperty'])
       triples += triple(EB['q%sa%s' % (keyid, v)], RDFS['label'], "%s (%%)" % answers[v])

      country_uri = countries[str(nationid)]['uri']

      for r in data:
        (month, year) = r[0].split('/')
        observation_uri = "%s/observation/%s/%s/%s/%s" % (BASE_URI, nationid, keyid, year, month)
        survey_uri = "%s/survey/%s/%s" % (BASE_URI, year, month)
        triples += triple(observation_uri, RDF['type'], QB['Observation'])
        triples += triple(observation_uri, RDFS['label'], 'All results from %s for "%s" in survey %s.%s' % (countries[str(nationid)]['label'], label, month, year))
        triples += triple(observation_uri, QB['dataSet'], dataset_uri)
        triples += triple(observation_uri, EB['survey'], survey_uri)
        triples += triple(observation_uri, EB['surveyMonth'], int(month))
        triples += triple(observation_uri, EB['surveyYear'], int(year))
        triples += triple(observation_uri, EB['question'], question_uri)
        triples += triple(observation_uri, EB['country'], country_uri)
        for v in range(1, len(r)):
          m = re.search("^\s*(.+)%", r[v])
          if m:
            triples += triple(observation_uri, EB['q%sa%s' % (keyid, v)], float(m.group(1)))

        survey_slice_uri = "%s/slice/%s/%s" % (BASE_URI, nationid, keyid)
        question_slice_uri = "%s/slice/%s/-/%s/%s" % (BASE_URI, nationid, year, month)
        country_slice_uri = "%s/slice/-/%s/%s/%s" % (BASE_URI, keyid, year, month)

        triples += triple(survey_slice_uri, RDFS['label'], 'All results from %s for "%s"' % (countries[str(nationid)]['label'], label))
        triples += triple(survey_slice_uri, QB['observation'], observation_uri)
        triples += triple(survey_slice_uri, EB['question'], question_uri)
        triples += triple(survey_slice_uri, EB['country'], country_uri)

        triples += triple(question_slice_uri, RDFS['label'], "All results from %s in survey %s.%s" % ( countries[str(nationid)]['label'], month, year))
        triples += triple(question_slice_uri, QB['observation'], observation_uri)
        triples += triple(question_slice_uri, EB['survey'], survey_uri)
        triples += triple(question_slice_uri, EB['country'], country_uri)

        triples += triple(country_slice_uri, RDFS['label'], 'All results for "%s" in survey %s.%s' % (label, month, year))
        triples += triple(country_slice_uri, QB['observation'], observation_uri)
        triples += triple(country_slice_uri, EB['survey'], survey_uri)
        triples += triple(country_slice_uri, EB['question'], question_uri)

        triples += triple(dataset_uri, QB['slice'], survey_slice_uri)
        triples += triple(dataset_uri, QB['slice'], question_slice_uri)
        triples += triple(dataset_uri, QB['slice'], country_slice_uri)

        self.file.write(triples)
        triples = ''
      return item      
        

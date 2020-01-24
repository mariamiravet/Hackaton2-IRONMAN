#BEAM Hackathon 2
# Mapa carril bici

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.options.pipeline_options import SetupOptions

from elasticsearch import Elasticsearch 

import json
import utm

import time

class Carrilbici(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
        
        #{"empty_slots":17,"extra":{"address":"Economista Gay - Constituci\xc3\xb3n","banking":false,"bonus":false,"last_update":1578482815000,"slots":20,"status":"OPEN","uid":136},"free_bikes":3,"id":"1f6b81722ca23ce520f77207b868afa9","latitude":39.4899091610835,"longitude":-0.375701108044157,"name":"136_CALLE_ECONOMISTA_GAY","timestamp":"2020-01-08T11:34:20.782000Z"}'
        #Example carril bici
        #{ "type": "Feature", "properties": { "estado": "2" }, "geometry": { "type": "LineString", "coordinates": [ [ 725988.92, 4374518.932 ], [ 725848.226, 4374645.306 ] ] } }
        
        item = json.loads(element)
        #print(item)
        #print("*************** COORDINATES ************")
        #print(item['coordinates'])
        street=item['geometry']['coordinates']
        #print(item['geometry'])
        #print(street)
        #street=street[1:-1]    #Eliminar [ ] del inicio y final
        #street_list=street.split('],')  #Convierto string en lista
        street_list_c=[]

        for i in street:     #Para cada coordenada la convierto a coordenada utm
            #i=i.replace("]","")
            #i=i.replace("[","")
            #i_list=list(i.split(","))
            lat_c=i[0]
            lon_c=i[1]
            lat,lon=utm.to_latlon(float(lat_c),float(lon_c),30,'U')
            #i_c="["+str(lat)+","+str(lon)+"]"
            i_c=[lon,lat]
            #i_c.append(lon)
            #i_c.append(lat)
            street_list_c.append(i_c)
        #print(">>>>>>>>> Street_list_c: ")
        #print(street_list_c)
        item['geometry']['coordinates']=street_list_c
        
        if not item['properties']['estado'].isnumeric():
            item['properties']['estado']=0
        else: 
            item['properties']['estado']=int(item['properties']['estado'])
        
        print("STREET_LIST_C: ")
        print(street_list_c)
        ts=time.gmtime()
        tmsp=time.strftime("%Y-%m-%d %H:%M:%S",ts)
                
        return [{'timestamp':tmsp,
                 'estado':item['properties']['estado'],
                 'geometry':item['geometry'],
                 'type':item['geometry']['type'],
                 'coordinates':{"type":"linestring", "coordinates":street_list_c}
                 }]


class IndexDocument(beam.DoFn):
   
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='carrilbici',body=element)
        
        print(res)
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  #1 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackaton2-miguelangel/topics/carrilbici',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackaton2-miguelangel/subscriptions/streamingcarrilbici',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #3 Replace your hackathon-edem with your project id 
  google_cloud_options.project = 'hackaton2-miguelangel'
  google_cloud_options.job_name = 'jobcarrilbici'
 
  # Uncomment below and add your bucket if you want to execute on Dataflow
  #google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  #google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  #pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  Carril_bici = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  Carril_bici = ( Carril_bici | beam.ParDo(Carrilbici()))
  
  Carril_bici | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  Carril_bici | 'Carril bici' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
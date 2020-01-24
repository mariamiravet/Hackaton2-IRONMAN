#BEAM Hackathon 2

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


class AparcaBici(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
        
        #{"empty_slots":17,"extra":{"address":"Economista Gay - Constituci\xc3\xb3n","banking":false,"bonus":false,"last_update":1578482815000,"slots":20,"status":"OPEN","uid":136},"free_bikes":3,"id":"1f6b81722ca23ce520f77207b868afa9","latitude":39.4899091610835,"longitude":-0.375701108044157,"name":"136_CALLE_ECONOMISTA_GAY","timestamp":"2020-01-08T11:34:20.782000Z"}'
        #Example medida trafico
        #{ "type": "Feature", "properties": { "plazas": "10", "tipo": "A - Horquilla", "id": "19105" }, "geometry": { "type": "Point", "coordinates": [ 728356.434999999590218, 4371533.649 ] } }
        
        item = json.loads(element)
        print(item)
        print("*************** COORDINATES ************")
        coord=utm.to_latlon(float(item['geometry']['coordinates'][0]),float(item['geometry']['coordinates'][1]),30,'U')
        latitude=coord[0]
        longitude=coord[1]
       
        return [{'type':item['type'],
                 'plazas':float(item['properties']['plazas']),
                 'tipo':item['properties']['tipo'],
                 'id':item['properties']['id'],
                 'geotype':item['geometry']['type'],
                 'coordinates':item['geometry']['coordinates'], 
                 'location':str(latitude)+","+str(longitude)   
                 }]


class IndexDocument(beam.DoFn):
   
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='estacionbici',body=element)
        
        print(res)
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  #1 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackathon-ramon/topics/estacionbici',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackathon-ramon/subscriptions/streamingestbici',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #3 Replace your hackathon-edem with your project id 
  google_cloud_options.project = 'hackathon-ramon'
  google_cloud_options.job_name = 'jobestacionbici'
 
  # Uncomment below and add your bucket if you want to execute on Dataflow
  #google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  #google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  #pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  biciEstaciones = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  biciEstaciones = ( biciEstaciones | beam.ParDo(AparcaBici()))
  
  biciEstaciones | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  biciEstaciones | 'Bici Medida Traffic' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
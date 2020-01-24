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


class IntensidadTraffic(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
        
        #{"empty_slots":17,"extra":{"address":"Economista Gay - Constituci\xc3\xb3n","banking":false,"bonus":false,"last_update":1578482815000,"slots":20,"status":"OPEN","uid":136},"free_bikes":3,"id":"1f6b81722ca23ce520f77207b868afa9","latitude":39.4899091610835,"longitude":-0.375701108044157,"name":"136_CALLE_ECONOMISTA_GAY","timestamp":"2020-01-08T11:34:20.782000Z"}'
        #Example medida trafico
        #{"modified":"2020-01-12T16:57:00+01:00","intensidad":"110769","punto_medida":"1163","angulo":"239","ycoord":"725877.214999999850988","xcoord":"4373375.19","uri":"http://apigobiernoabiertortod.valencia.es/apirtod/datos/intensidad_espiras/503d6e8c7aa6fd749e05d682a728d54d.json"}
        
        item = json.loads(element)
        print(item)
        print("*************** COORDINATES ************")
        coord=utm.to_latlon(float(item['ycoord']),float(item['xcoord']),30,'U')
        latitude=coord[0]
        longitude=coord[1]
       
        return [{'timestamp':item['modified'],
                 'intensidad':float(item['intensidad']),
                 'punto_medida':item['punto_medida'],
                 'angulo':item['angulo'],
                 'ycoord':float(item['ycoord']),
                 'xcoord':float(item['xcoord']), 
                 'uri':item['uri'],
                 'location':str(latitude)+","+str(longitude)   
                 }]


class IndexDocument(beam.DoFn):
   
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='medidatrafico',body=element)
        
        print(res)
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  #1 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackaton2-miguelangel/topics/medidatrafico',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackaton2-miguelangel/subscriptions/streamingmedtraf',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #3 Replace your hackathon-edem with your project id 
  google_cloud_options.project = 'hackaton2-miguelangel'
  google_cloud_options.job_name = 'jobmedidatrafico'
 
  # Uncomment below and add your bucket if you want to execute on Dataflow
  #google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  #google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  #pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  biciTraffic = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  biciTraffic = ( biciTraffic | beam.ParDo(IntensidadTraffic()))
  
  biciTraffic | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  biciTraffic | 'Bici Medida Traffic' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
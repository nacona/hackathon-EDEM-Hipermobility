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
from datetime import datetime
import pytz


class Espiras(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
        
        
        item = json.loads(element)
        
        x = float(item['ycoord']) # El ayto de Valencia se ha equivocado. La coordenada X equivale a la Y, y viceversa.
        y = float(item['xcoord'])
        return [{'timestamp':datetime.now(tz = pytz.utc).isoformat(), # Guardamos el momento de indexado. Sirve para el filtro temporal del gráfico.
                 'intensidad':item['intensidad'],
                 'punto_medida':item['punto_medida'],
                 'angulo':item['angulo'],
                 'xcoord':float(item['ycoord']),
                 'ycoord':float(item['xcoord']),
                 #'location':str(item['xcoord'])+","+str(item['ycoord'])
                 'location':str(utm.to_latlon(x, y, 30,'N')[0])
                         +","+str(utm.to_latlon(x, y, 30,'N')[1])
            
                 }]

class IndexDocument(beam.DoFn):
   
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='espiras',body=element)
        
        print(res)
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  #1 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackaton-superlopez/topics/espiras',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackaton-superlopez/subscriptions/streaming2',
                      help='Input Subscription')
  
     
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #3 Replace your hackathon-edem with your project id 
  google_cloud_options.project = 'hackaton-superlopez'
  google_cloud_options.job_name = 'myjob'
 
  # Uncomment below and add your bucket if you want to execute on Dataflow
  #google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  #google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  #pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  espiras = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  espiras = ( espiras | beam.ParDo(Espiras()))
  
  espiras | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  espiras | 'Espiras Stored' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
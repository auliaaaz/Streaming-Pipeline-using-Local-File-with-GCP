import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'your-path/keyfile.json'

input_subscription = 'projects/your-project-id/subscriptions/your-subscription-name'
output_table = 'your-project-id:your-dataset-name.your-table-name'

output_schema = (
    'extract_run_date:STRING, '
    'randomized_id:INTEGER, '    
    'ccrb_received_year:INTEGER, '  
    'days_between_incident_date:INTEGER, '  
    'case_type:STRING, '
    'complaint_received_place:STRING, '
    'complaint_received_mode:STRING, '
    'borough_of_incident:STRING, '
    'patrol_borough_of_incident:STRING, '
    'reason_for_initial_contact:STRING'
)

class CustomParsing(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        record = json.loads(element.decode('utf-8'))

        # Transform the record from Pub/Sub to match the BigQuery schema
        new_record = {
            'extract_run_date': record.get('Extract Run Date'),
            'randomized_id': record.get('Randomized Id'),
            'ccrb_received_year': record.get('CCRB Received Year'),
            'days_between_incident_date': record.get('Days Between Incident Date and Received Date'),
            'case_type': record.get('Case Type'),
            'complaint_received_place': record.get('Complaint Received Place'),
            'complaint_received_mode': record.get('Complaint Received Mode'),
            'borough_of_incident': record.get('Borough Of Incident'),
            'patrol_borough_of_incident': record.get('Patrol Borough Of Incident'),
            'reason_for_initial_contact': record.get('Reason For Initial Contact')
        }
        
        # Handling integers value     
        for field in ['randomized_id', 'ccrb_received_year', 'days_between_incident_date']:
            field_value = str(new_record.get(field, '')).replace(',', '').strip()
            if field_value.isdigit():
                new_record[field] = int(field_value)
            else:
                new_record[field] = None
                
        yield new_record


def run():
    options = PipelineOptions(
        streaming=True,
        runner='DataflowRunner',
        project='your-project-id',
        job_name='pipeline-nyc',
        temp_location='gs://your-bucket-name/your-temp-folder',
        region='asia-southeast2'    # Adjust with your project region
    )

    with beam.Pipeline(options=options) as p:
        (
            p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
              | 'CustomParse' >> beam.ParDo(CustomParsing())
              | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                  output_table,
                  schema=output_schema,
                  write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
              )
        )

if __name__ == "__main__":
    run()

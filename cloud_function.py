from googleapiclient.discovery import build

def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "exemplary-vista-428214-t8"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "covid_data_job",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath" : "gs://meta-data-covidcountries/uds.js",
        "JSONPath" : "gs://meta-data-covidcountries/bigquery_schema.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "exemplary-vista-428214-t8:Covid.Covid_Data",
        "inputFilePattern": "gs://covid-data-countries/covid_data.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://meta-data-covidcountries",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

# Streaming-Pipeline-using-Local-File-with-GCP
The source code for the [article](https://medium.com/@auliaaz/building-a-streaming-pipeline-in-google-cloud-pub-sub-dataflow-and-bigquery-df8ebe1460fa) explaining how to create a streaming pipeline in GCP utilising BigQuery, Dataflow, Pub/Sub, using local data file is available in this repository.

![alt text](https://github.com/auliaaaz/Streaming-Pipeline-using-Local-File-with-GCP/blob/main/arch.png)

### Set Up
Install the necessary requirements:
```
pip install apache-beam
pip install apache-beam[gcp]
pip install google-api-python-client
```
and don't forget to run this to interact with Google Cloud SDK
```
gcloud auth application-default login
```
### Text Classification with BigQuery ML
For full guide, you can refer to [this](https://cloud.google.com/bigquery/docs/text-embedding-semantic-search#console_1). But before feeding the model, I do some simple preprocessing in BigQuery.
1. Change the term to its abbreviation
   
   Because in the dataset, there are some term that need to expand to make a better understanding to the model.
   
    ```
    UPDATE `semantic_search_tutorial.uniqueContent`
    SET content = 
      CASE 
      WHEN content LIKE '%CV%' THEN REPLACE(content, 'CV','complainant or victim')
      WHEN content LIKE '%C/V%' THEN REPLACE(content, 'C/V', 'complainant or victim')
      ELSE content
      END
    WHERE content LIKE '%CV%' OR content LIKE '%C/V%'
    ```
    ```
    UPDATE `semantic_search_tutorial.uniqueContent`
    SET content = REPLACE(content, 'PD', 'police department')
    WHERE content LIKE '%PD%'
    ```
    ```
    UPDATE `semantic_search_tutorial.uniqueContent`
    SET content = REPLACE(content, 'bldg', 'building')
    WHERE content LIKE '%bldg%'
    ```
    ```
    UPDATE `semantic_search_tutorial.uniqueContent`
    SET content = REPLACE(content, 'VTL', 'vehicle and traffic law')
    WHERE content LIKE '%VTL%'
    ```
    ```
    UPDATE `semantic_search_tutorial.uniqueContent`
    SET content = REPLACE(content, 'PCT', 'police precinct')
    WHERE content LIKE '%PCT%'
    ```
    ```
    UPDATE `semantic_search_tutorial.rawData`
    SET content = REPLACE(content, 'EDP', 'Emotionally Disturbed Person')
    WHERE content LIKE '%EDP%'
    ```
3. Only applying embedding model to unique value first (this can reduce time processing to make embedding text) then 'JOIN' it with all rows data with
   
    ```
    CREATE OR REPLACE TABLE semantic_search_tutorial.uniqueContent_embedding AS (
      SELECT *
      FROM ML.GENERATE_TEXT_EMBEDDING(
        MODEL `semantic_search_tutorial.embedding_model`,
        TABLE semantic_search_tutorial.uniqueContent,
        STRUCT(TRUE AS flatten_json_output)
      )
    );
    ```
    then 'JOIN' it with the rawData 
    ```
    CREATE OR REPLACE TABLE semantic_search_tutorial.rawData_embedding AS (
      SELECT r.*, u
      FROM `semantic_search_tutorial.uniqueContent_embedding` r
      JOIN `semantic_search_tutorial.rawData` u
    
      ON r.content = u.content
    );
    ```
   

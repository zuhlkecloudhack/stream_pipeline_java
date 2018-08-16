stream_pipeline_java
---------------------
Streaming Pipeline for Zühlke's Cloud Challenge. This example illustrates the Java SDK for [Apache Beam](https://beam.apache.org/). It sets up a streaming pipeline executing on [Google Cloud Dataflow](https://cloud.google.com/dataflow/) which listens to events on a [Google Pub/Sub](https://cloud.google.com/pubsub/) topic, writes incoming events to [Google Cloud Storage](https://cloud.google.com/storage/) and [Google BigQuery](https://cloud.google.com/bigquery/) and sends a response to another Google Pub/Sub topic.


#### Preconditions:
 - JDK >= 1.8
 - [Google Cloud SDK](https://cloud.google.com/sdk/) installed and configured against a project


#### Credentials:
```
rm ~/.config/gcloud/application_default_credentials.json
gcloud auth login
gcloud auth application-default login
```

#### Running the pipeline
To run the pipeline locally you have to provide these arguments:
```
--project=<your_google_cloud_project_name>
```

To run the pipeline on [Google Cloud Dataflow](https://cloud.google.com/dataflow/) provide these program arguments
```
--project=<your_googl_cloud_project_name> 
--tempLocation=gs://<some_temp_bucket>/staging 
--region=europe-west1 
--runner=DataflowRunner 
--maxNumWorkers=<maximum number of workers>
```

#### Publish message into topic:
The pipeline listens for incoming events on a pub/sub topic. In order to publish messages to this topic use the gcloud command line interface.
```bash
gcloud beta pubsub topics publish <your topic name> --message '
{
    "flight-number" : "CH5634",
    "message" : "Fly me to the moon",
    "message-type" : "INFO",
    "timestamp" : "2012-04-23T18:25:43.511Z"
}'
```

#### Results
After starting the pipeline and submitting messages to the topic you should check the corresponding Google Cloud Dataflow job and see something similar to this:

![pipeline result](https://github.com/zuhlkecloudhack/stream_pipeline_java/raw/master/src/main/resources/cloud-dataflow-streaming-pipeline.png "Cloud Dataflow Pipeline")
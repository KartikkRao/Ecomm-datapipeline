# Ecomm-datapipeline
End to end ecomm datapipeline using aws S3 , gluespark , snowflake and airflow.


overall process as follows:
1) Data is posted onto the s3 bucket in raw format. 
2) Transformation job is written in gluespark wherein it is configured to store the transformed data in another folder.
3) Snowpipe is created with the s3 location where my transformed data is stored so as soon as new data comes it will be ingested inside main table of snowflake.
4) Snowflake querry to update/insert into my dim_fact model.
5) All this is orchestrated using airflow:-
    a) In airflow when dag starts to run it first checks whether csv file is present in my raw folder if yes then triggers the next job.
    b) Next job is glue spark once transformation is completed and data goes to that folder it takes 2 to 3min to ingest data in snowflake.
    c) So next task is sleep that is to wait for 3min.
    d) after 3min my sql query is snowflake is run.

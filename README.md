1. Create a bucket to store the outout or temporary data and upload the csv file

2. Create a bigquery dataset and a table with a schema
   dataset: dataset_demo
   Table: output-sql
   Schema: "products:STRING", "total_sales:FLOAT",

3. Install the virtualenv module, create a virtual environment, and then activate it:

pip3 install virtualenv 
python3 -m virtualenv env 
source env/bin/activate

4. Install Apache Beam SDK
   pip3 install apache-beam[gcp]

5. Upload the python script and run the pipeline in the cloudshell
   python dataflow_sql.py
--project=techlanders-internal /
--temp_location=gs://sandeep-apache/temp /
--input=gs://sandeep-apache/input.csv /
--output=gs://sandeep-apache/output /
--bq_table=techlanders-internal.dataset_demo.output-sql /
--region us-east1 /
--zone=us-east1-b /
--runner=DataflowRunner

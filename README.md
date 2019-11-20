# gcp-py-framework
Install Python 3.X
Install following Python modules
https://cloud.google.com/sdk/install
pip install google-cloud-pubsub
pip install google-cloud-storage
pip install google-cloud-bigquery

bqIngestFramework.py is the initial version framework created to ingest the data from a file to BigQuery.
1. This is a resuable framework which requires an environment value and param file path as input arguments while invoking the script
2. Provide all key value pairs in the param file.
3. Param file name is constant as processParam_<ENV>.py
4. Place processParam_<ENV>.py under respective subject area.
5. Sample invoke command  python <filepath>/bqIngestFramework.py --env DEV --paramFilePath <file path>

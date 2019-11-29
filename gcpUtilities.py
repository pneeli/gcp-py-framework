#SCRIPT NAME: gcpUtilities.py
#CREATE DATE: 2019-11-21
#AUTHOR     : Pavankumar Neeli
#DESCRIPTION: This is a common script to invoke various GCP - GCS/BQ functions, these can be imported to other
#             scripts.
#             Following functions were created:
#             1. Authenicate to GCP with the Service Account provided
#             2. Function to upload a file to GCS upLoadGcsApi(Invokes Storage API's), upLoadGcs(Command line)
#             3. Function to do Big Query load runBqLoadApi(Invokes Python API's), runBqLoad(Command line)
#             4. Function to do Big Query extract runBqExtractApi(Invokes Python API's)
#             5. Function to download a file from GCS to local file system downLoadFromGcsApi(Invokes Storage API's),
#                downLoadFromGcsApi(Command Line)
#             6. Function to create a stored procedure by reading ddl from a file
#             7. Function to call a stored procedure with or with out arguments, please pass the arguments while
#                calling the definition
#MODIFIED DATE: 
#MODIFICATIONS: 
#*******************************************************************************************************************#
import os
import subprocess
import sys
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
from google.cloud import pubsub_v1



#*************TIMESTAMP DEFINITION START****************#
'''
Return Current Timestamp
'''
def currTimestamp():
	timestamp = datetime.now().strftime('%Y-%m-%d:%H:%M:%S.%f')
	return timestamp
#*************TIMESTAMP DEFINITION DONE****************#

#*************DATE DEFINITION START****************#
'''
Return Current Date
'''
def currDate():
	date = datetime.now().strftime('%Y-%m-%d')
	return date
#*************DATE DEFINITION DONE****************#

#*************GCP BQ AUTHENTICATION DEFINITION START****************#
'''
Function to Authenicate with Big Query
'''
def gcpBqAuth(gcpEnv):
	log(f"Authenticating to Google Big Query","INFO")
	log(f"GOOGLE_APPLICATION_CREDENTIALS = {gcpAuthFileName}{gcpEnv}.{gcpAuthFileExt}","INFO")
	os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f"{gcpAuthFilePath}{gcpAuthFileName}{gcpEnv}.{gcpAuthFileExt}"
	log(f"Choosing Processing Project:{gcpProcessingProj}{gcpEnv}","INFO")
#************GCP BQ AUTHENTICATION DEFINITION END******************#

#*************GCS FILE API UPLOAD DEFINITION START****************#
'''
Function to Upload file to GCS via Python API
'''
def upLoadGcsApi(env, absFile, path, bucketName, subDir, currDate):
	log(f"GCP Bucket to upload file:{bucketName}/{subDir}/'data_in'/{currDate}","INFO")
	log(f"Local file name:{absFile}","INFO")
	storage_client = storage.Client()
	bucket = storage_client.get_bucket(bucketName)
	bucketFolder = subDir + '/' + 'data_in' + '/' + currDate
	file=absFile.replace(path,'')
	log(f"Uploading file to GCS....","INFO")
	blob = bucket.blob(bucketFolder + '/' + file)
	try:
		blob.upload_from_filename(absFile)
		log(f"File uploaded GCS!!!","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"Failed to upload file to GCS!!!","ERROR")
		raise
#*************GCS FILE API UPLOAD DEFINITION END****************#

#*************GCS FILE COMMAND LINE UPLOAD DEFINITION START****************#
'''
Function to Upload file to GCS via Command line
'''
def upLoadGcs(env, absFile, bucketName, subDir, currDate):
	log(f"GCP Bucket to upload file:{bucketName}/{subDir}/'data_in'/{currDate}","INFO")
	log(f"Local file name {absFile}","INFO")
	cmnd = 'gsutil -m cp '+ absFile + ' ' + 'gs://' + bucketName + '/' + subDir + '/' + 'data_in' + '/' + currDate + '/'
	log(f"Command executing for uploading file to GCS: {cmnd}","INFO")
	try:
		subprocess.call('gsutil -m cp ' + absFile + " " + "gs://" + bucketName + '/' + subDir + '/' + 'data_in' + '/' + currDate + '/',shell=True)
		log(f"GCS Upload Completed: {cmnd}","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"GCS Upload Failed","ERROR")
		raise
#*************GCS FILE COMMAND LINE UPLOAD DEFINITION END****************#

#*************BQ LOAD API DEFINITION DONE****************#
'''
Function to run Big Query Load from a file present on GCS via Python API
'''
def runBqLoadApi(env, header, format, replaceFlg, fileName, path, delimiter, bucket, subDir, directory, proj, dataset, tableName):
	fileFormat = format.upper()
	file=fileName.replace(path,'')
	projectName=proj + env
	client = bigquery.Client(project=projectName)
	table_ref = client.dataset(dataset).table(tableName)
	job_config = bigquery.LoadJobConfig()
	job_config.skip_leading_rows = header
	if fileFormat == 'CSV':
		job_config.source_format = bigquery.SourceFormat.CSV
	else:
		job_config.source_format = ''
	job_config.fieldDelimiter = delimiter
	if replaceFlg.lower()=='y':
		job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
	else:
		job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
	uri = 'gs://' + bucket + '/' + subDir + '/' + 'data_in' + '/' + directory + '/' + file
	try:
		load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
		log(f"BQLoad Completed: {cmnd}","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"BQLoad Failed","ERROR")
		raise
#*************BQ LOAD API DEFINITION DONE****************#

#*************BQ LOAD COMMAND LINE DEFINITION DONE****************#
'''
Function to Big Query load from a file on GCS using command line
'''
def runBqLoad(env, header, format, replaceFlg, fileName, path, delimiter, bucket, subDir, directory, project, dataset, tableName):
	file=fileName.replace(path,'')
	headerRow = ' --skip_leading_rows=' + str(header) if header!='' else header
	srcFormat = ' --source_format=' + format if format!='' else format
	fDelimiter = ' --field_delimiter='+delimiter if delimiter!='' else delimiter
	rFlg = ' --replace ' if replaceFlg.lower()=='y' else ' --noreplace '
	bqTable = project + env + ':' + dataset + '.' + tableName
	gcsFilePath = ' gs://' + bucket + '/' + subDir + '/' + 'data_in' + '/' + directory + '/' + file
	cmnd='bq --location=US load' + headerRow + srcFormat+ fDelimiter + rFlg + bqTable + gcsFilePath
	log(f"Running bqload: {cmnd}","INFO")
	try:
		subprocess.call(cmnd,shell=True)
		log(f"BQLoad Completed: {cmnd}","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"BQLoad Failed","ERROR")
		raise
#*************BQ LOAD COMMAND LINE DEFINITION END****************#

#*************GCP BQ RUN API QUERY DEFINITION START****************#
'''
Function to run Big Query Sqls using Python API
'''
def runBqQueryApi(env, filePath, proj, sqlFile, disposition):
	bqSqlFile=filePath + sqlFile
	projectName=proj + env
	log(f"SQL File to execute : {bqSqlFile}","INFO")
	catQuery=subprocess.check_output(['cat',bqSqlFile])
	query=str(catQuery).replace('b','').replace('\\n','').replace("'",'')
	job_config = bigquery.QueryJobConfig()
	if disposition == 'append':
		job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
	elif disposition == 'truncate':
		job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
	log(f"Query executing: {query}","INFO")
	client = bigquery.Client(project=projectName)
	try:
		query_job = client.query(query,location="US")
		query_job.result()
		log(f"Query Execution Completed","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"Failed to execute the Query","ERROR")
		raise
#*************GCP BQ API QUERY DEFINITION END****************#

#************GCP BQ EXPORT DEFINITION START******************#
'''
Function to extract data from a table to GCS via Python API
'''
def runBqExtractApi(env, compress, headerFlg, fileFormat, fileName, extension, delimiter, bucket, subDir, directory, proj, dataset, tableName):
	destination_uri = "gs://" + bucket + '/' + subDir + '/' + 'data_out' + '/' + directory + '/' + fileName + extension
	projectName = proj + env
	log(f"Extracting data from following table name:{projectName}.{dataset}.{tableName}","INFO")
	client = bigquery.Client(project=projectName)
	dataset_ref = client.dataset(dataset, project=projectName)
	table_ref = dataset_ref.table(tableName)
	job_config = bigquery.job.ExtractJobConfig()
	if compress.lower()=='y':
		log(f"File compression to GZIP flag is ON","INFO")
		job_config.compression = bigquery.Compression.GZIP
	job_config.field_delimiter = delimiter
	job_config.destination_format = fileFormat
	if headerFlg.lower()=='true':
		log(f"Print Header flag is ON","INFO")
		job_config.print_header='True'
	else:
		log(f"Print Header flag is OFF","INFO")
		job_config.print_header='False'
	try:
		extract_job = client.extract_table(table_ref,destination_uri,location='US',job_config=job_config)
		extract_job.result()
		log(f"Data extraction completed to following GCS path: {destination_uri}","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"Failed to extract data from:{projectName}.{dataset}.{tableName}","ERROR")
		raise
#************GCP BQ EXPORT DEFINITION DONE******************#

#*************GCS FILE API DOWNLOAD DEFINITION START****************#
'''
Function to download file from GCS to local file system via Python API
'''
def downLoadFromGcsApi(env, fileName, path, bucketName, subDir, currDate):
	log(f"GCP Bucket to download file from:{bucketName}/{subDir}/'data_out'/{currDate}","INFO")
	log(f"Local file name:{filename}","INFO")
	srcBlobName = subDir + '/' + 'data_in' + '/' + currDate + '/' + fileName
	storage_client = storage.Client()
	bucket = storage_client.get_bucket(bucketName)
	blob = bucket.blob(srcBlobName)
	absFileName=path + '/' + fileName
	try:
		log(f"Downloading file from GCS to local file system....","INFO")
		blob.download_to_filename(absFileName)
		log(f"File download to local file system is done!!!","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"Failed to download the file to local file system....","ERROR")
		raise
#*************GCS FILE API DOWNLOAD DEFINITION END****************#

#*************GCS FILE COMMAND LINE DOWNLOAD DEFINITION START****************#
'''
Function to download file to GCS to local file system via Command line
'''
def downLoadFromGcs(env, fileName, path, bucketName, subDir, currDate):
	log(f"GCP Bucket to download file from:{bucketName}/{subDir}/data_out/{currDate}","INFO")
	absFileName=path + '/' + fileName
	log(f"Local file name {absFileName}","INFO")
	cmnd = 'gsutil cp ' + 'gs://' + bucketName + '/' + subDir + '/' + 'data_out' + '/' + currDate + '/' + fileName + ' ' + absFileName
	log(f"Command executing for download file from GCS: {cmnd}","INFO")
	try:
		log(f"Downloading file from GCS to local file system....","INFO")
		subprocess.call('gsutil cp ' + 'gs://' + bucketName + '/' + subDir + '/' + 'data_out' + '/' + currDate + '/' + fileName + ' ' + 
			absFileName,shell=True)
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"Failed to download the file to local file system....","ERROR")
		raise
#*************GCS FILE COMMAND LINE DOWNLOAD DEFINITION END****************#

#*************GCP BQ CREATE OR REPLACE STORED PROCEDURE DEFINITION START****************#
'''
Function to create or replace a store procedure by reading from a .sql file using Python API
'''
def createBqStoredProcApi(env, proj, filePath, sqlFile):
	spSqlFile = filePath + sqlFile
	projectName = proj + env
	log(f"Stored Procedure path and filename : {spSqlFile}","INFO")
	catQuery=subprocess.check_output(['cat',spSqlFile])
	query=str(catQuery).replace('b','').replace('\\n','').replace("'",'')
	job_config = bigquery.QueryJobConfig()
	log(f"Stored Procedure to execute:{query}","INFO")
	client = bigquery.Client(project=projectName)
	try:
		query_job = client.query(query,location="US")
		query_job.result()
		log(f"Stored Procedure Created or Replaced","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"Failed to create or replace stored procedure","ERROR")
		raise
#*************GCP BQ CREATE OR REPLACE STORED PROCEDURE DEFINITION END****************#

#*************GCP BQ EXECUTE STORED PROCEDURE DEFINITION START****************#
'''
Function to execute a store procedure by reading using Python API
'''
def runBqStoredProcApi(env, proj, dataSet, storedProc, arguments=''):
	projectName = proj + env
	query="CALL " + '`' + projectName + '`' + '.' + dataSet + '.' + storedProc + "(" + arguments + ")"
	log(f"Command to call a stored procedure : {query}","INFO")
	job_config = bigquery.QueryJobConfig()
	client = bigquery.Client(project=projectName)
	try:
		query_job = client.query(query,location="US")
		query_job.result()
		log(f"Executed Stored Procedure Successfully","INFO")
	except Exception as e:
		log(f"Exception : {e}","ERROR")
		log(f"Failed to run stored procedure","ERROR")
		raise
#*************GCP BQ EXECUTE STORED PROCEDURE DEFINITION END****************#

#*************LOG DEFINITION START****************#
def log(msg,msgType):
	print(currTimestamp(),f"[{msgType}]gcpUtilities.py:",msg)
#*************LOG DEFINITION END****************#

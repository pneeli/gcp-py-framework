#SCRIPT NAME: fsUtilities.py
#CREATE DATE: 2019-11-25
#AUTHOR     : Pavankumar Neeli
#DESCRIPTION: This is a common script to deal with file system related activities.
#             1. Function to check for the file presence at the specified path
#             2. Function to decompress of gzip file.
#MODIFIED DATE: 
#MODIFICATIONS: 
#*******************************************************************************************************************#
import os
import subprocess
import sys
import argparse
import glob
import gzip
import shutil
import logging


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

#*************CHECK FOR DATA/TRG FILE DEFINITION START****************#
'''
Function to check for the file presence at the specified path
'''
def checkFilePresence(filepath, filename, extension=''):
	retFile=glob.glob(filepath + '/' + filename + '*' + extension + '*')
	log(f"File found:{retFile}","INFO")
	if retFile!=[]:
		log(f"File found with the specified pattern:{filename}*{extension}","INFO")
	else:
		log(f"File not found with the specified pattern:{filename}*{extension}","ERROR")
		sys.exit(1)
	return str(retFile).replace('[','').replace(']','').replace("'",'')
#*************CHECK FOR DATA/TRG FILE DEFINITION DONE****************#

#*************DECOMPRESS FILE DEFINITION START****************#
'''
Function to Decompress the specified file.
'''
def deCompressFile(absFilePath):
	gzFilePath=absFilePath
	ungzFilePath=gzFilePath.replace(fileZipExtn,'')
	with gzip.open(gzFilePath, 'rb') as f_in:
		with open(ungzFilePath, 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)
#*************DECOMPRESS FILE DEFINITION DONE****************#

#*************LOG DEFINITION START****************#
def log(msg,msgType):
	print(currTimestamp(),f"[{msgType}]fsUtilities.py:",msg)
#*************LOG DEFINITION END****************#
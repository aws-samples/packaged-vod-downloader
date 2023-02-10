#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# DownloadVod.py
# - Validate input
# - Identify all the files in the asset to be copied
# - Identify what (if any) files have already been copied to the destination
# - Copy missing files to destination
# - Return result containing:
#   - Number of assets copied
#   - State: Complete | Incomplete

import os
import time
import urllib3
import concurrent.futures
import queue
import random
import boto3
import json
from pprint import pprint
from urllib.parse import urlparse
from HlsVodAsset import HlsVodAsset
from DashVodAsset import DashVodAsset
import logging

logger = logging.getLogger()
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

# Constants
LAMBDA_MIN_TIME_REMAINING_TRIGGER = 1 * 120 * 1000 # ms
MAX_NUMBER_THREAD = 20

poolManager = None
s3 = boto3.resource('s3')

#TODO: Progress update
#TODO: Add support for CDN Auth headers
#TODO: Potentially send SNS before any fatal exit (this could provide a more human readable error)


def loadUrlWorker(caller, url, authHeaders):

  injectErrors = False

  if injectErrors:
    if int(random.random() * 10000.0) % 37 == 0:
      # Corrupt url (delete one characater at random) to test retry logic
      pos = random.randint(1,len(url)-2)
      url = url[0:pos] + url[pos+1:]
  try:
    response = poolManager.request( "GET", url, headers=authHeaders )
  except IOError as urlErr:
    urlPayload = None
    print('I/O error fetching', url)
    print(urlErr)
    return (urlPayload, None)

# Here if urlopen succeeded.  Check http result code.  Anything other than
# 200 (success) is returned to caller as an error.

  if response.status != 200:
    urlPayload = None
    contentType = None
    print('http error', response.status, 'fetching', url)
  else:

# Get the payload.

    expectedLen = int(response.headers['Content-Length'])
    contentType = response.headers['Content-Type']
    if injectErrors:
      if int(random.random() * 10000.0) % 29 == 0:
        # Corrupt expectedLen (increase by 1) to test retry
        expectedLen += 1
    urlPayload = response.data
    receivedLen = len(urlPayload)
    if receivedLen != expectedLen:
      print(caller+':', url, 'expected', expectedLen, '; received', receivedLen)
      urlPayload = None
      contentType = None

  return (urlPayload, contentType)

def loadUrl(caller, url, authHeaders):

  retryCount = 3
  retryInterval = 2
  attempt = 0

  while attempt < retryCount:
    (urlPayload, contentType) = loadUrlWorker(caller, url, authHeaders)
    if urlPayload != None:
      return (urlPayload, contentType)
    attempt += 1
    time.sleep(retryInterval)

  print(caller, 'failed to load after', attempt, 'attempts: ', url)
  return (None, None)

def fetchSegments(n, baseUrl, fetchQ, s3, destBucket, destPrefix, acl, authHeaders):
# This is the function invoked for each thread created.

# Reads a segment name from the queue, and calls loadUrl to fetch
# it.  If no success, skips the segment and moves on to the next.
# The fetch rate is determined by the queue fill rate.
#
# If the fetch succeeds, writes the segment to the specifed S3 bucket.

  downloadedSegments = []
  segment = ''
  skippedSegments = []
  while segment != '#QUIT':
    segment = fetchQ.get()

    if segment != '#QUIT':

      # fetch segment here
      segmentBase = segment.split('?')[0]   # Strip off any query params
      segmentBase = '/' + segmentBase.replace(baseUrl, "")
      
      t = time.time()
      logger.debug("Attempting to download: %s" % segment)
      (segmentData, contentType) = loadUrl('fetchSegments', segment, authHeaders)
      if segmentData == None:
        logger.debug("No segment data downloaded")
        logger.debug("'%s' fetch attempt failed; skipping" % segmentBase)
        skippedSegments.append(segment)
      else:
        writeBucket(s3, destBucket, destPrefix, segmentBase, segmentData, contentType, acl)
        downloadedSegments.append(segment)
        # if verbose:
        #   print('Thread', n, segmentBase, contentType, '{:2.2f}'.format(time.time() - t), 's')
    fetchQ.task_done()

  return {
    "downloadedSegments": downloadedSegments,
    "totalDownloadedSegments": len(downloadedSegments),
    "skippedSegments": skippedSegments,
    "totalSkippedSegments": len(skippedSegments)
  }


def writeBucket(s3, destBucket, destPrefix, objectName, content, contentType, acl):
# Writes content to prefix+objectName in bucketName.  Failures are fatal.

 try:
  logger.debug("DEBUG: Writing segment to: s3://%s/%s" % (destBucket, destPrefix+objectName))
  s3.Bucket(destBucket).put_object(Key=destPrefix+objectName, Body=content, ContentType=contentType, ACL=acl)
 except Exception as s3Err:
   print('Fatal:  error writing to S3')
   print('Bucket: ', destBucket, ' Asset ID:', destPrefix, ' Object:', objectName, ' ACL:', acl)
   print(s3Err)
   os._exit(3)


def fetchStream(event, context):

  # - Validate input
  # - Identify all the files in the asset to be copied
  # - Identify what (if any) files have already been copied to the destination
  # - Copy missing files to destination
  # - Return result containing:
  #   - Number of assets copied
  #   - State: Complete | Incomplete

  logger.info("Event:")
  pprint(event)
  logger.info("Context:")
  pprint(context)

  # Validate Inputs
  validationResult = validateInputs(event, context)
  if validationResult['status'] != 200:
    return {
      'status': 500,
      'message': validationResult['message'],
      'result': { "status": "FAILED" }
    }

  # Setup variables
  masterManifestUrl           = event['source_url']
  destBucket                  = event['destination_bucket']
  packagingConfig             = event['packaging_config']
  packaging_group_auth_header = None
  if 'packaging_group_auth_header' in event.keys():
    packaging_group_auth_header = event['packaging_group_auth_header']
  rpsLimit          = event['rpsLimit']
  acl               = 'private'
  numThreads        = 5
  destPath          = None
  if 'destination_path' in event.keys():
    destPath   = event['destination_path']
  if 'numThreads' in event.keys():
    numThreads = event['numThreads']

  # Parse passed in Auth Header
  authHeaders = None
  if packaging_group_auth_header:
    authHeaders = parseAuthHeaders(packaging_group_auth_header)

  # Initialize urllib3 Pool Manager
  global poolManager
  poolManager = urllib3.PoolManager( maxsize=numThreads )

  # Parse origin asset manifests
  vodAsset = None
  vodAssetType = None
  try:
    ( vodAsset, vodAssetType ) = parseVodAssetManifests( masterManifestUrl, authHeaders )
  except IOError as urlErr:
    return {
      'status': 500,
      'message': "%s: Unable access manifest." % repr(urlErr),
      'result': { "status": "FAILED" }
    }
  except Exception as e:
    print(repr(e))
    return {
      'status': 500,
      'message': "Unhandled Exception. Check logs",
      'result': { "status": "FAILED" }
    }
  else:
    if vodAssetType == "UnsupportedFormat":
      return {
        'status': 500,
        'message': "Manifest is of an unsupported format",
        'result': { "status": "FAILED" }
      }


  # Inspect destination to check which (if any) files have already been copied)
  preExistingObjects = listObjectsAtDestination( s3, destBucket, destPath )

  logger.info( "Source asset contains %d resources" % len(vodAsset.allResources) )
  logger.info( "%d resources need to be downloaded" % (len(vodAsset.allResources)-len(preExistingObjects)) )

  #TODO: Could possible default the number of threads to a minimum of one thread per variant

  # Create queue
  fetchQ = queue.Queue()

  # Main processing loop starts here.
  numQueuedObject = 0
  fetchStart = time.time()

  # We can use a with statement to ensure threads are cleaned up promptly
  threadResults = {}
  with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_NUMBER_THREAD) as executor:

    # Start the load operations and mark each future with its thread number
    logger.info('Starting %d threads' % numThreads)
    threadNumbers = list(range(1, numThreads+1))
    threads = {executor.submit(fetchSegments, n, vodAsset.commonPrefix, fetchQ, s3, destBucket, destPath, acl, authHeaders): n for n in threadNumbers}

    ( stopBeforeTimeout, numberQueuedObjects ) = queueObjectsToFetch(preExistingObjects, vodAsset.allResources, vodAsset.commonPrefix, fetchQ, rpsLimit, context)

    # All media segments have been queued.  Now send QUIT token to worker threads, 
    # and wait for them to complete
    for t in range(numThreads):
      fetchQ.put('#QUIT')

    for thread in concurrent.futures.as_completed(threads):
      threadNumber = threads[thread]
      logger.info("Getting result for thread %d" % threadNumber)
      try:
        threadResults[threadNumber] = thread.result()
      except Exception as e:
        logger.error('Failed to get a result for thread %d: %s' % (threadNumber, e))
      else:
        logger.info("Thread %s complete" % (threadNumber))
        # pprint(threadResults[threadNumber])

  # Aggregate results
  aggResults = {
    'downloadedSegments'      : [],
    'skippedSegments'         : [],
    'totalDownloadedSegments' : 0,
    'totalSkippedSegments'    : 0
  }
  for threadNumber, threadResult in threadResults.items():
    # aggResults['downloadedSegments'].extend(threadResult['downloadedSegments'])
    aggResults['skippedSegments'].extend(threadResult['skippedSegments'])
    aggResults['totalDownloadedSegments'] = aggResults['totalDownloadedSegments'] + threadResult['totalDownloadedSegments']
    aggResults['totalSkippedSegments'] = aggResults['totalSkippedSegments'] + threadResult['totalSkippedSegments']
  
  # Set status on result
  finalExistingObjects = listObjectsAtDestination( s3, destBucket, destPath )
  aggResults['objectsAtS3Dest'] = len(finalExistingObjects)

  logger.info("Objects found at destination = %d" % len(finalExistingObjects))
  logger.info("VOD All Resources = %d" % len(vodAsset.allResources))
  if len(finalExistingObjects) == len(vodAsset.allResources):
    # Stream successfully copied
    aggResults['status'] = "COMPLETE"
  elif stopBeforeTimeout == True:
    # Stream not successfully copied yet. Lamdba stopped before timeout
    # Re-run lambda to continue copying
    aggResults['status'] = "LAMBDA_TIMEOUT"
  else:
    # Stream has not successfully copied and has tried to copy all resources
    # Some resources may have failed to be copied an been skipped
    aggResults['status'] = "INCOMPLETE"

  # Calculate the percentage of files which have been copied to destination
  aggResults['progressPercentage'] = str(round((len(finalExistingObjects)/len(vodAsset.allResources))*100,2))

  returnVal = {
      'status': 200,
      'message': aggResults['status'],
      'result': aggResults,
      'asset' : {
        's3Location': getMasterManifestLocation(vodAsset, destBucket, destPath),
        'packagingConfiguration': packagingConfig,
        'type': vodAssetType
      }
    }

  return returnVal

def getMasterManifestLocation(vodAsset, destBucket, destPath):
  # Determine the location of the master manifest for the asset
  masterManifest = vodAsset.masterManifest
  s3Prefix = "s3://%s/%s/" % (destBucket, destPath)
  s3MasterManifest = masterManifest.replace( vodAsset.commonPrefix, s3Prefix )
  return s3MasterManifest

def queueObjectsToFetch(preExistingObjects, allResources, commonPrefix, fetchQ, rpsLimit, context):

  # Adds objects to be downloaded to the queue at a throttled rate to ensure
  # origin is not overloaded.
  # Function receives a list of objects to be stored on S3 and will skip any
  # objects which already exists. Objects are identified as already existing
  # if they are in the 'preExistingObjects' list

  stopBeforeTimeout = False
  numQueuedObject = 0
  for object in allResources:
    
    # Strip commonPrefix from URL to compare with S3 Key
    objectKey = object.replace(commonPrefix, "")

    if context:
      if context.get_remaining_time_in_millis() < LAMBDA_MIN_TIME_REMAINING_TRIGGER:
        stopBeforeTimeout = True
        break
    
    # Add object to fetch queue
    if objectKey not in preExistingObjects:

      # TODO: Add logic here to prevent too many objects being added to the queue rapidly
      # to the point where they will not be processed before the lambda timeout
      # Suggest setting a max queue size (say 20). If the queue is larger then the max
      # size the process should sleep. While in this loop periodic checks will be required
      # to take not of the LAMBDA_MIN_TIME_REMAINING_TRIGGER and break the loop if required
      # to prevent a timeout
      # fetchQ.qsize()

      logger.debug("Adding resource to queue: %s" % objectKey)
      fetchQ.put(object)
      numQueuedObject += 1

      if rpsLimit > 0:
        time.sleep(1/float(rpsLimit))

    # else:
    #   logger.debug("Not added to queue: %s" % objectKey)
  
  return (stopBeforeTimeout, numQueuedObject)


def parseVodAssetManifests( assetUrl, authHeaders ):
  # Process the passed in manifest file and return a vodAsset object
  # with all the data necessary to download all the parts of the stream
  # Returns a data structure containing the parse information and
  # the type of asset

  parsedUrl = urlparse(assetUrl)
  vodAsset                  = None
  if parsedUrl.path.endswith('.m3u8'):
    vodAssetType = 'hls'
    vodAsset = HlsVodAsset(assetUrl, authHeaders)

  elif parsedUrl.path.endswith('.mpd'):
    vodAssetType = 'dash'
    vodAsset = DashVodAsset(assetUrl, authHeaders)

  else:
    vodAssetType = 'UnsupportedFormat'

  return ( vodAsset, vodAssetType )

def listObjectsAtDestination( s3Resource, destBucket, destPath ):
  # Populates a list of all objects in destination path.  This is used to know
  # which assets we can skip in the event of a restart or when using the 
  # 'continue' feature when hosted by Lambda

  existingObjects = []

  logger.info("Checking for object with prefix: s3://%s/%s" % (destBucket, destPath))

  for obj in s3Resource.Bucket(destBucket).objects.filter(Prefix=destPath):
    name = obj.key[len(destPath):]
    existingObjects.append(name)

  logger.info("Found %d objects exist with '%s' prefix" % ( len(existingObjects), destPath) )

  return existingObjects

def parseAuthHeaders( input ):
  logger.info("Parsing Auth Headers:")
  pprint(input)
  authHeaders = None
  try:
    authHeaders = json.loads(input)
  except Exception as e:
    logger.error("Failed to parse passed in 'packaging_group_auth_header'. This should be a json string.")
    raise(e)

  # Check for MediaPackageCDNIdentifier and change to X-MediaPackage-CDNIdentifier if found
  # This to address use case where customer has followed the example on the MediaPackage
  # documentation (https://docs.aws.amazon.com/mediapackage/latest/ug/cdn-auth.html)
  if "MediaPackageCDNIdentifier" in authHeaders.keys():
    # Add equivalent value with modified key.
    authHeaders["X-MediaPackage-CDNIdentifier"] = authHeaders["MediaPackageCDNIdentifier"]
    del authHeaders["MediaPackageCDNIdentifier"]

  logger.info("Manipulated Auth Headers:")
  pprint(authHeaders)

  return authHeaders

def validateInputs(event, context):

  # Check URL, bucket and Asset ID are specified (and not blank)
  mandatoryParams = ['source_url', 'destination_bucket', 'destination_path']
  for param in mandatoryParams:
    if (param not in event.keys()) or (event[param] is None) or (event[param] == ''):
      message = "Fatal: Parameter '%s' must be specified" % param
      logger.error(message)
      return {
        'status': 500,
        'message': message
      }

  # Check S3 Bucket exists
  client = boto3.client('s3')
  destBucket = event['destination_bucket']
  try:
    response = client.list_objects_v2(
      Bucket=destBucket,
      MaxKeys=1
    )
  except Exception as s3Err:
    message = "Fatal: Unable to verify '%s' bucket exists" % destBucket
    logger.error(message)
    return {
      'status': 500,
      'message': message
    }

  return {
    'status': 200,
    'message': ''
  }

def parseCmdLine():

  # Command-line options (order unimportant)

  # -o <origin-url> [Required]
  # -d <destination-bucket> [Required]
  # -a <asset-ID> [Required]
  # -t <thread-count>
  # -r <rps-max>
  
  import argparse
  
  parser = argparse.ArgumentParser(description='VOD Downloader')
  
  argdefs = []
  
  # List of tuples:  (option, dispname, type, action, helptext, required)
  
  argdefs.append(('-i', 'URL', str, 'store', 'URL for HLS endpoint on origin server', True))
  argdefs.append(('-b', 'bucket', str, 'store', 'Destination S3 bucket name', True))
  argdefs.append(('-d', 'path', str, 'store', 'Destination path', True))
  argdefs.append(('-p', 'packaging-config', str, 'store', 'Packaging Configuration name', False))
  # argdefs.append(('-r', None, None, 'store_true', 'Removes ad content, leaving markers intact', False))
  
  for arg in argdefs:
    if arg[3] == 'store':
      parser.add_argument(arg[0], metavar=arg[1], type=arg[2], action=arg[3], help=arg[4], required=arg[5])
    else:
      parser.add_argument(arg[0], action=arg[3], help=arg[4], required=arg[5])
  
  args = parser.parse_args()
  
  event = {}
  
  event['source_url']         = args.i
  event['destination_bucket'] = args.b
  event['destination_path']   = args.d
  event['packaging_config']   = args.p
  event['numThreads']         = 5
  event['rpsLimit']           = 1000

  return event



if __name__ == '__main__':  # Lambda __name__ isn't '__main__'

  event = parseCmdLine()

  context = None
  result = fetchStream(event, context)
  pprint(result)
  if result['status'] != 200:
    logger.info(result['message'])
  logger.info('Command-line invocation complete')
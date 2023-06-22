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

import os
import urllib3
from pprint import pprint
from urllib.parse import urlparse
import re

http = urllib3.PoolManager()

class HlsVodAsset:
  def __init__(self, masterManifest, authHeaders=None):
    self.masterManifest = masterManifest
    self.masterManifestContentType = None
    self.variantManifests   = None
    self.variantManifestsData = {}
    self.mediaSegmentList  = []
    self.commonPrefix = None
    self.allResources = None
    self.authHeaders = authHeaders

    self.parseHlsVodAsset()

# Function will parse variant manifest and extract a list of all media and init segments
# media and init segments will store absolute URLs for segments in mediaSegmentList
  def parseHlsVodAsset( self ):

    # Retrieve Master Manifest
    (masterManifestBody, self.masterManifestContentType) = getManifest( self.masterManifest, self.authHeaders )
    #TODO: If manifest is None, raise error

    # Parse Master Manifest
    self.variantManifests = parseMasterManifest( self.masterManifest, masterManifestBody )

    # For each variant manifest
    for variant in self.variantManifests:

      # Retrieve Variant Manifest
      (variantManifestBody, variantContentType) = getManifest( variant, self.authHeaders )
      self.variantManifestsData[variant] = {
        "body": variantManifestBody,
        "contentType": variantContentType
      }

      # Parse Variant Manifest
      segments = parseVariantManifest( variant, variantManifestBody )
      self.mediaSegmentList.extend(segments)

    # Determine commonPrefix across all resource
    allResources = [ self.masterManifest ]
    allResources.extend(self.variantManifests)
    allResources.extend(self.mediaSegmentList)

    # Duplicates need to be removed from the list of all segments.
    # This may not strictly be necessary for HLS/CMAF streams but cannot hurt.
    allResourcesSet = set(allResources)
    uniqueResources = list(allResourcesSet)

    self.allResources = uniqueResources

    # Set common prefix
    # The common prefix must end with '/' to indicate this is a path and does not include
    # the start of the name of the files. For example, if all the resources of an asset start
    # with 'asset1' and the content is stored in 'my/asset/path' the common prefix should be
    # 'my/asset/path/' not 'my/asset/path/asset1'
    commonPrefix = os.path.commonprefix( uniqueResources )
    if not commonPrefix.endswith('/'):
      # Strip off anything to the right of the last '/' because this component represents
      # the common string at the start of the filenames
      unwantedPathSuffix = os.path.basename(commonPrefix)
      pathSuffix = re.compile( unwantedPathSuffix + '$' )
      commonPrefix = pathSuffix.sub('', commonPrefix)
    self.commonPrefix = commonPrefix

    return


def getManifest( url, authHeaders ):

  contentType = None
  try:
    response = http.request( "GET", url, headers=authHeaders )
  except IOError as urlErr:
    print("Exception occurred while attempting to get: %s" % url )
    print(repr(urlErr))
    urlPayload = None
    raise(urlErr)

  if response.status != 200:
    urlPayload = None
    print('http error', response.status, 'fetching', url)
  else:
    urlPayload = response.data
    contentType = response.headers['Content-Type']
    # Some packagers set the manifest type incorrectly.
    # This needs to be corrected if the content type is 'binary/octet-stream'
    if contentType == 'binary/octet-stream':
      print("Content type was '%s', overriding to '%s'" % (contentType, 'application/x-mpegURL'))
      contentType = 'application/x-mpegURL'

    # Not all servers return a 'Content-Length' header. If available it is worth checking
    if 'Content-Length' in response.headers.keys():
      expectedLen = int(response.headers['Content-Length'])
      receivedLen = len(urlPayload)
      if receivedLen != expectedLen:
        print('HlsVodAsset: ', url, 'expected', expectedLen, '; received', receivedLen)
        urlPayload = None

  if not( urlPayload is None ):
    urlPayload = urlPayload.decode('utf-8')
    # Check if it's an HLS manifest
    if urlPayload[0:7] != '#EXTM3U':
      print('Not an HLS manifest:', url)
      os._exit(2)

  return ( urlPayload, contentType )


# Function will parse master manifest and extract a list of all the variant manifests
# Variant manifest URLs will be stored in list in 'variantManifest'
def parseMasterManifest( masterManifestUrl, masterManifestBody ):

  variantsDict = {}
  for line in masterManifestBody.splitlines():
    name = None

    # Parse line starting with EXT-X-MEDIA
    # e.g. EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio_0",CHANNELS="2",NAME="und",LANGUAGE="und",DEFAULT=YES, \
    #      AUTOSELECT=YES,URI="bf4fc289ea7a4a9a8030bfdfb6dd8180/75449fe7ed1a492880193067011
    if line.startswith('#EXT-X-MEDIA:') or line.startswith('#EXT-X-I-FRAME-STREAM-INF:'):
      line = line.split(':', 1)[1]
      mediaDict = {}

      # Add EXT-X-MEDIA properties to data set 
      for keyVal in re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', line):
        (key, val) = keyVal.split('=', 1)
        mediaDict[key] = val.strip('"')
      name = mediaDict['URI']

    elif line == "":
      # Skip blank lines
      next

    # Parse lines which do not start with a comment
    # e.g. ../../../bf4fc289ea7a4a9a8030bfdfb6dd8180/75449fe7ed1a49288019306701174382/index_1_0.ts
    elif line[0] != '#':
      name = line

    # Add key to dict if it has not been seen before
    absoluteUrl = name
    if name and name.startswith("http"):
      absoluteUrl = name
    elif name:
      absoluteUrl = normalizeUrl("%s/%s" % (os.path.dirname(masterManifestUrl), name))

    if not (absoluteUrl is None or absoluteUrl in variantsDict.keys()):
      variantsDict[absoluteUrl] = 1
    
  variants = list(variantsDict.keys())

  return variants



# Normalises url and removes additional '..' notations
def normalizeUrl( url ):

  o = urlparse(url)
  absPath = os.path.normpath( o.path )
  absUrl = "%s://%s%s" % (o.scheme, o.netloc, absPath)

  return absUrl


# Function will parse variant manifest and extract a list of all media and init segments
# media and init segments will store absolute URLs for segments in mediaSegmentList
def parseVariantManifest( variantManifestUrl, variantManifestBody ):

  segmentsDict = {}
  for line in variantManifestBody.splitlines():
    name = None

    # Parse line starting with EXT-X-MEDIA
    # e.g. #EXT-X-MAP:URI="../../../a595fd669f4349e1846efee6e27ccfa8/bba5843ebf8f41619348551669b17f47/index_video_1_init.mp4"
    if line.startswith('#EXT-X-MAP:') or line.startswith('#EXT-X-I-FRAME-STREAM-INF:'):
      line = line.split(':', 1)[1]
      mediaDict = {}

      # Add EXT-X-MEDIA properties to data set 
      for keyVal in re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', line):
        (key, val) = keyVal.split('=', 1)
        mediaDict[key] = val.strip('"')
      name = mediaDict['URI']

    # Parse lines which do not start with a comment
    # e.g. ../../../bf4fc289ea7a4a9a8030bfdfb6dd8180/75449fe7ed1a49288019306701174382/index_1_0.ts
    elif line[0] != '#':
      name = line

    # Add key to dict if it has not been seen before
    absoluteUrl = name
    if name and name.startswith("http"):
      absoluteUrl = name
    elif name:
      absoluteUrl = normalizeUrl("%s/%s" % (os.path.dirname(variantManifestUrl), name))

    if not (absoluteUrl is None or absoluteUrl in segmentsDict.keys()):
      segmentsDict[absoluteUrl] = 1
    
  segments = list(segmentsDict.keys())

  return segments
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

from mpegdash.parser import MPEGDASHParser
import os
import urllib3
from isodate import parse_duration
from datetime import datetime
from pprint import pprint
from urllib.parse import urlparse

http = urllib3.PoolManager()

# Supported Manifest
# Compact Time/Number with Timeline
# - AdaptationSet contains:
#   - Segment Template
#   - One or more representations (without Segment Template)
#
# Full Time/Number with Timeline
# - AdaptationSet contains:
#  - One or more representations
#  - Each representations contains Segment Template

class DashVodAsset:
  def __init__(self, masterManifest, authHeaders=None):
    self.masterManifest = masterManifest
    self.masterManifestContentType = None
    self.mediaSegmentList  = []
    self.commonPrefix = None
    self.allResource = None
    self.authHeaders = authHeaders

    self.parseDashVodAsset()

  # Function will parse variant manifest and extract a list of all media and init segments
  # media and init segments will store absolute URLs for segments in mediaSegmentList
  def parseDashVodAsset( self ):

    mediaSegments = []

    # Retrieve Manifest
    (masterManifestBody, self.masterManifestContentType) = getManifest( self.masterManifest, self.authHeaders )
    mpd = MPEGDASHParser.parse(masterManifestBody)
    mpdBaseUrl = os.path.dirname(self.masterManifest)

    # loop over periods
    periodCounter = 1
    for period in mpd.periods:

      print("Starting processing Period %d ... " % periodCounter)
      # loop over all the adaptation sets in the period

      adaptationSetCounter = 1
      for adaptationSet in period.adaptation_sets:

        print("Starting processing AdaptationSet %d with MimeType '%s'" % (adaptationSetCounter, adaptationSet.mime_type))
        
        listOfSegments = getAdaptationSetSegmentList(mpdBaseUrl, adaptationSet, period)
        mediaSegments.extend(listOfSegments)
      
        print("Finished processing AdaptationSet %d." % adaptationSetCounter)

        # Increment AdaptationSet Counter
        adaptationSetCounter = adaptationSetCounter + 1

      print("Finished processing Period %d." % periodCounter)

      # Increment Period Counter
      periodCounter = periodCounter + 1

    # Identify common base URL for all resources
    allSegments = list(mediaSegments)
    allSegments.append(self.masterManifest)

    self.commonPrefix =  os.path.commonprefix( allSegments )
    self.mediaSegmentList = mediaSegments
    self.allResources = allSegments

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
    expectedLen = int(response.headers['Content-Length'])
    receivedLen = len(urlPayload)
    if receivedLen != expectedLen:
      print('DashVodAsset: ', url, 'expected', expectedLen, '; received', receivedLen)
      urlPayload = None

  if not( urlPayload is None ):
    urlPayload = urlPayload.decode('utf-8')

  return ( urlPayload, contentType )

# Normalises url and removes additional '..' notations
def normaliseUrl( url ):

  o = urlparse(url)
  absPath = os.path.normpath( o.path )
  absUrl = "%s://%s%s" % (o.scheme, o.netloc, absPath)

  return absUrl


def getAdaptationSetSegmentList(mpdBaseUrl, adaptationSet, period):

  mediaSegments = []
  
  for representation in adaptationSet.representations:
    print("Processing Representation %s:" % representation.id)

    # Get segment Template
    # Segment template may be defined in representation or at the Adaptation set level
    segmentTemplates = None
    if representation.segment_templates:
      segmentTemplates = representation.segment_templates
    elif adaptationSet.segment_templates:
      segmentTemplates = adaptationSet.segment_templates
    else:
      print("Unable to find Segment Template for Representation %s" % representation.id)
      exit(1)

    # Assumption there is only one segment template per adaptation set
    if len(segmentTemplates) > 1:
      print("Unsupported DASH Manifest format. Maximum of one segment template per adaptations set")
      exit(2)

    ############################
    # Process Media Files
    ############################

    # Extract Media Segment template and fill in any required parameters (e.g. representation id)
    segmentTemplate = segmentTemplates[0]
    mediaSegmentTemplate = segmentTemplate.media
    if "$RepresentationID$" in mediaSegmentTemplate:
      mediaSegmentTemplate = mediaSegmentTemplate.replace("$RepresentationID$", str(representation.id))
    print("Media Segment Template: %s" % mediaSegmentTemplate)

    # Generate a list of media files to be downloaded
    # Segment Templates do not exist for some renditions (e.g. 'image/jpeg')
    # For these renditions a segment timeline is inferred from the SegmentTemplate
    mediaSegmentTimes = None
    if segmentTemplate.segment_timelines:
      mediaSegmentTimes = getSegmentTimeline( segmentTemplate )
    else:
      mediaSegmentTimes = getInferredSegmentTimeline( segmentTemplate.start_number, segmentTemplate.timescale, segmentTemplate.duration, period.duration )

    mediaSegmentsForRepresentation = getMediaSegmentList( mediaSegmentTemplate, segmentTemplate.start_number, mediaSegmentTimes, mpdBaseUrl )

    ############################
    # Process Init File (it exists for this rendition)
    ############################
    if segmentTemplate.initialization:

      # Extract init segment template and fill in any required parameters (e.g. representation id)
      initSegmentTemplate = segmentTemplate.initialization
      if "$RepresentationID$" in initSegmentTemplate:
        initSegmentTemplate = initSegmentTemplate.replace("$RepresentationID$", str(representation.id))
      print("Init Segment Template: %s" % initSegmentTemplate)
      # Add init file to resource list
      absInitSegmentTemplate = normaliseUrl(mpdBaseUrl + '/' + initSegmentTemplate)

      # Append init files to list of media files to be downloaded
      mediaSegmentsForRepresentation.append(absInitSegmentTemplate)
    else:
      print("Skipping init file as there is no init for '%s' representation" % representation.id)

    # Append list of files to be downloaded as part of this adaptation set
    mediaSegments.extend(mediaSegmentsForRepresentation)

  return mediaSegments

# Generate list of media segments
def getMediaSegmentList( mediaSegmentTemplate, startNumber, mediaSegmentTimes, mpdBaseUrl ):
  mediaSegments = []

  for t in mediaSegmentTimes:
    resource = mediaSegmentTemplate

    # Handle both Time with Timeline and Number with timeline mpd formats
    if "$Time$" in resource:
      # Time with Timeline mpd
      resource = resource.replace("$Time$", str(t))
    else:
      # Number with Timeline mpd
      resource = resource.replace("$Number$", str(startNumber))
      startNumber = startNumber + 1

    absResource = normaliseUrl(mpdBaseUrl + '/' + resource)
    mediaSegments.append(absResource)

  return mediaSegments

# Uses the segment template to generate a list of segment times
def getSegmentTimeline( segmentTemplate ):

  segmentTimelines = segmentTemplate.segment_timelines

  # Iterate over the segment components to create a list of the times for segments to download 
  mediaSegmentTimes = []
  segmentTimelineComponents = segmentTimelines[0].Ss
  for segmentTimelineComponent in segmentTimelineComponents:

    t = segmentTimelineComponent.t # time
    d = segmentTimelineComponent.d # duration
    r = segmentTimelineComponent.r # repeats

    # add first segment
    # print("MediaSegmentTime: %d" % t)
    mediaSegmentTimes.append(t)

    # add any repeat segments
    if not (r is None):
      for x in range(1,r+1):
        # print("MediaSegmentTime (r): %s" % str(t + x*d))
        mediaSegmentTimes.append(t + x*d)

  return mediaSegmentTimes

# Infers a segment timeline if not explicitly defined
def getInferredSegmentTimeline( startNumber, timescale, segmentTemplateDuration, periodDuration ):

  # Approach used here is to calculate segment size (i.e. segment template duration divided by timescale)
  # Dividing the duration of the period by the segment size should give the correct number of assets

  # Calculate segement size
  segmentSize = float(segmentTemplateDuration)/float(timescale)

  # Get period duration
  periodDuration = parse_duration(periodDuration).total_seconds()

  # Calculcate number of segments in period
  numberSegments = int(periodDuration / segmentSize)

  # Create array listing the segment numbers
  segmentTimelineNumbers = list(range(startNumber, startNumber+numberSegments))

  return segmentTimelineNumbers
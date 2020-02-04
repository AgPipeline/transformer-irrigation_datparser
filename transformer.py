"""Transformer to load irrigation CSV file to GeoStreams (TERRA REF)
"""

import argparse
import datetime
import json
import logging
import os
from urllib.parse import urlparse
from typing import Optional, Union
from parser import parse_file
import requests

import terrautils.sensors as sensors

import configuration
import transformer_class

# The default batch size of uploaded records
DEFAULT_BATCH_SIZE = 3000

# Irrigation file and sensor related definitions
IRRIGATION_FILENAME_START = 'flowmetertotals'
IRRIGATION_FILENAME_END = '.csv'
IRRIGATION_SENSOR_TYPE = 4

# Clowder and GeoStreams related definitions
CLOWDER_DEFAULT_URL = os.environ.get('CLOWDER_URL', 'https://terraref.ncsa.illinois.edu/clowder/')
CLOWDER_DEFAULT_KEY = os.environ.get('CLOWDER_KEY', '')
GEOSTREAMS_API_URL_PARTICLE = 'api/geostreams'

# Default terraref site
TERRAREF_SITE = os.environ.get('TERRAREF_SITE', 'ua-mac')


class __internal__:
    """Class for functionality to only be used by this file"""
    def __init__(self):
        """Initializes class instance"""

    @staticmethod
    def get_file_to_load(file_list: list) -> Optional[str]:
        """Returns the name of the file to load from the list of files
        Arguments:
            file_list: the list of file names to look through
        Return:
            Returns the first found file that matches the searching criteria
        """
        for one_file in file_list:
            if os.path.basename(one_file).startswith(IRRIGATION_FILENAME_START) and one_file.endswith(IRRIGATION_FILENAME_END):
                return one_file
        return None

    @staticmethod
    def terraref_sensor_display_name(sensor_name: str, site_name: str) -> str:
        """Returns the display name associated with the sensor
        Arguments:
            sensor_name: the name of the sensor to look up
            site_name: the name of the site to use during lookup
        Return:
            Returns the display name associated with the sensor
        """
        cur_sensor = sensors.Sensors(base='', station=site_name, sensor=sensor_name)
        return cur_sensor.get_display_name()

    @staticmethod
    def get_geostreams_api_url(base_url: str, url_particles: Union[str, tuple, None]) -> str:
        """Returns the URL constructed from parameters
        Arguments:
            base_url: the base URL (assumes scheme:[//authority] with optional path)
            url_particles: additional strings to append to the end of the base url
        Return:
            Returns the formatted URL (guaranteed to not have a trailing slash)
        """
        def url_join(base_url: str, url_parts: tuple) -> str:
            """Internal function to create URLs in a consistent fashion
            Arguments:
                base_url: the starting part of the URL
                url_parts: the parts of the URL to join
            Return:
                The formatted URL
            """
            built_url = ''
            base_parse = urlparse(base_url)
            if base_parse.scheme:
                built_url = base_parse.scheme + '://'
            if base_parse.netloc:
                built_url += base_parse.netloc + '/'
            if base_parse.path and not base_parse.path == '/':
                built_url += base_parse.path.lstrip('/') + '/'

            joined_parts = '/'.join(url_parts).replace('//', '/').strip('/')

            return built_url + joined_parts

        if not url_particles:
            return url_join(base_url, tuple(GEOSTREAMS_API_URL_PARTICLE))

        if isinstance(url_particles, str):
            return url_join(base_url, (GEOSTREAMS_API_URL_PARTICLE, url_particles))

        formatted_particles = tuple(str(part) for part in url_particles)
        return url_join(base_url, tuple(GEOSTREAMS_API_URL_PARTICLE) + formatted_particles)

    @staticmethod
    def _common_geostreams_name_get(clowder_url: str, clowder_key: str, url_endpoint: str, name_query_key: str, name: str) -> \
            Optional[dict]:
        """Common function for retrieving data from GeoStreams by name
        Arguments:
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            url_endpoint: the endpoint to query (URL particle appended to the base URL, eg: 'streams')
            name_query_key: the name of the query portion of URL identifying the name to search on (eg: 'stream_name')
            name: the name to search on
        Return:
            Returns the found information, or None if not found
        """
        url = __internal__.get_geostreams_api_url(clowder_url, url_endpoint)
        params = {name_query_key: name}
        if clowder_key:
            params['key'] = clowder_key

        logging.debug("Calling geostreams url '%s' with params '%s'", url, str(params))
        resp = requests.get(url, params)
        resp.raise_for_status()

        for one_item in resp.json():
            if 'name' in one_item and one_item['name'] == name:
                logging.debug("Found %s '%s' = [%s]", name_query_key, name, one_item['id'])
                return one_item

        return None

    @staticmethod
    def common_geostreams_create(clowder_url: str, clowder_key: str, url_endpoint: str, request_body: str) -> Optional[str]:
        """Common function for creating an object in GeoStreams
        Arguments:
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            url_endpoint: the endpoint to query (URL particle appended to the base URL, eg: 'streams')
            request_body: the body of the request
        Return:
            Returns the ID of the created object or None if no ID was returned
        """
        url = __internal__.get_geostreams_api_url(clowder_url, url_endpoint)
        if clowder_key:
            url = url + '?key=' + clowder_key

        result = requests.post(url,
                               headers={'Content-type': 'application/json'},
                               data=request_body)
        result.raise_for_status()

        result_id = None
        result_json = result.json()
        if isinstance(result_json, dict) and 'id' in result_json:
            result_id = result_json['id']
            logging.debug("Created GeoStreams %s: id = '%s'", url_endpoint, result_id)
        else:
            logging.debug("Call to GeoStreams create %s returned no ID", url_endpoint)

        return result_id

    @staticmethod
    def get_sensor_by_name(sensor_name: str, clowder_url: str, clowder_key: str) -> Optional[dict]:
        """Returns the GeoStreams sensor information retrieved from Clowder
        Arguments:
            sensor_name: the name of the data sensor to retrieve
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
        Return:
            Returns the information on the sensor or None if the stream can't be found
        """
        return __internal__._common_geostreams_name_get(clowder_url, clowder_key, 'sensors', 'sensor_name', sensor_name)

    @staticmethod
    def get_stream_by_name(stream_name: str, clowder_url: str, clowder_key: str) -> Optional[dict]:
        """Returns the GeoStreams stream information retrieved from Clowder
        Arguments:
            stream_name: the name of the data stream to retrieve
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
        Return:
            Returns the information on the stream or None if the stream can't be found
        """
        return __internal__._common_geostreams_name_get(clowder_url, clowder_key, 'streams', 'stream_name', stream_name)

    @staticmethod
    def create_sensor(sensor_name: str, clowder_url: str, clowder_key: str, geom: dict, sensor_type: dict, region: str) -> str:
        """Create a new sensor in Geostreams.
        Arguments:
            sensor_name: name of new sensor to create
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            geom: GeoJSON object of sensor geometry
            sensor_type: JSON object with {"id", "title", and "sensorType"}
            region: region of sensor
        """
        body = {
            "name": sensor_name,
            "type": "Point",
            "geometry": geom,
            "properties": {
                "popupContent": sensor_name,
                "type": sensor_type,
                "name": sensor_name,
                "region": region
            }
        }

        return __internal__.common_geostreams_create(clowder_url, clowder_key, 'sensors', json.dumps(body))

    @staticmethod
    def create_stream(stream_name: str, clowder_url: str, clowder_key: str, sensor_id: str, geom: dict, properties=None) -> str:
        """Create the indicated GeoStream
        Arguments:
            stream_name: the name of the  data stream to retrieve
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            sensor_id: the ID of the sensor associated with the stream
            geom: the geometry of the stream to create
            properties: additional properties for the stream
        Return:
            The ID of the created stream
        """
        body = {
            "name": stream_name,
            "type": "Feature",
            "geometry": geom,
            "properties": {} if not properties else properties,
            "sensor_id": str(sensor_id)
        }

        return __internal__.common_geostreams_create(clowder_url, clowder_key, 'streams', json.dumps(body))

    @staticmethod
    def create_data_points(clowder_url: str, clowder_key: str, stream_id: str, data_point_list: list) -> None:
        """Uploads the data points to GeoStreams
        Arguments:
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            stream_id: the ID of the stream to upload to
            data_point_list: the list of data points to upload
        """
        body = {
            "datapoints": data_point_list,
            "stream_id": str(stream_id)
        }

        __internal__.common_geostreams_create(clowder_url, clowder_key, 'datapoints/bulk', json.dumps(body))


def add_parameters(parser: argparse.ArgumentParser) -> None:
    """Adds parameters
    Arguments:
        parser: instance of argparse.ArgumentParser
    """
    parser.add_argument('--batchsize', type=int, default=DEFAULT_BATCH_SIZE,
                        help="maximum number of data points to submit in one request (default %s)" % str(DEFAULT_BATCH_SIZE))
    parser.add_argument('--clowder_url', default=CLOWDER_DEFAULT_URL,
                        help="the url of the Clowder instance to access for GeoStreams (default '%s')" % CLOWDER_DEFAULT_URL)
    parser.add_argument('--clowder_key', default=CLOWDER_DEFAULT_KEY,
                        help="the key to use when accessing Clowder %s" %
                        ("(default: using environment value)" if CLOWDER_DEFAULT_KEY else ''))
    parser.add_argument('--site_override', default=TERRAREF_SITE,
                        help="override the site name (default '%s')" % TERRAREF_SITE)

    parser.epilog = "Processing one irrigation file at a time"

    # Here we specify a default metadata file that we provide to get around the requirement while also allowing overrides
    # pylint: disable=protected-access
    for action in parser._actions:
        if action.dest == 'metadata' and not action.default:
            action.default = ['/home/extractor/default_metadata.json']
            break


def check_continue(transformer: transformer_class.Transformer, check_md: dict) -> tuple:
    """Checks if conditions are right for continuing processing
    Arguments:
        transformer: instance of transformer class
        check_md: request specific metadata
    Return:
        Returns a tuple containing the return code for continuing or not, and
        an error message if there's an error
    """
    # pylint: disable=unused-argument
    if __internal__.get_file_to_load(check_md['list_files']()):
        return tuple([0])

    return -1, "No irrigation CSV file was found in list of files to process (file name must match '%s*%s')" % \
           (IRRIGATION_FILENAME_START, IRRIGATION_FILENAME_END)


def perform_process(transformer: transformer_class.Transformer, check_md: dict) -> dict:
    """Performs the processing of the data
    Arguments:
        transformer: instance of transformer class
        check_md: request specific metadata
    Return:
        Returns a dictionary with the results of processing
    """
    start_timestamp = datetime.datetime.now()
    all_files = check_md['list_files']()
    received_files_count = len(all_files)

    file_to_load = __internal__.get_file_to_load(all_files)

    # TODO: Get this from Clowder fixed metadata]
    main_coords = [-111.974304, 33.075576, 361]
    geom = {
        "type": "Point",
        "coordinates": main_coords
    }

    # Get the display name to use
    display_name = __internal__.terraref_sensor_display_name(configuration.TRANSFORMER_SENSOR, transformer.args.site_override)

    # Get sensor or create if not found
    sensor_data = __internal__.get_sensor_by_name(display_name, transformer.args.clowder_url, transformer.args.clowder_key)
    if not sensor_data:
        sensor_id = __internal__.create_sensor(display_name, transformer.args.clowder_url, transformer.args.clowder_key, geom,
                                               {
                                                   'id': 'MAC Met Station',
                                                   'title': 'MAC Met Station',
                                                   'sensorType': IRRIGATION_SENSOR_TYPE
                                               },
                                               'Maricopa')
    else:
        sensor_id = sensor_data['id']

    # Get stream or create if not found
    stream_name = "Irrigation Observations"
    stream_data = __internal__.get_stream_by_name(stream_name, transformer.args.clowder_url, transformer.args.clowder_key)
    if not stream_data:
        stream_id = __internal__.create_stream(stream_name, transformer.args.clowder_url, transformer.args.clowder_key, sensor_id, geom)
    else:
        stream_id = stream_data['id']

    # Load the records and loop through them
    logging.info("Processing file: '%s'", file_to_load)
    records = parse_file(file_to_load, main_coords)
    total_dp = 0
    data_point_list = []
    for record in records:
        data_point_list.append({
            "start_time": record['start_time'],
            "end_time": record['end_time'],
            "type": "Point",
            "geometry": record['geometry'],
            "properties": record['properties']
        })
        if len(data_point_list) > transformer.args.batchsize:
            logging.debug("Adding %s data points", str(len(data_point_list)))
            __internal__.create_data_points(transformer.args.clowder_url, transformer.args.clowder_key, stream_id, data_point_list)
            total_dp += len(data_point_list)
            data_point_list = []
    logging.debug("Remaining number of points: %s vs max: %s", str(len(data_point_list)), str(transformer.args.batchsize))
    if len(data_point_list) > 0:
        logging.debug("Adding %s remaining data points", str(len(data_point_list)))
        __internal__.create_data_points(transformer.args.clowder_url, transformer.args.clowder_key, stream_id, data_point_list)
        total_dp += len(data_point_list)

    return {'code': 0,
            configuration.TRANSFORMER_NAME: {
                'version': configuration.TRANSFORMER_VERSION,
                'utc_timestamp': datetime.datetime.utcnow().isoformat(),
                'processing_time': str(datetime.datetime.now() - start_timestamp),
                'num_files_received': str(received_files_count),
                'num_records_added': str(total_dp)
            }
            }

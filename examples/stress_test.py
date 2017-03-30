# Copyright 2017 InitialDLab
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
""" stress test example for sample server

This python program provides a way to easily test that the python
APIs are working.  The stress test consists of 4 activities:
1) repeatedly request a list of indexes in the sample server.
2) request a new index to be built.
3) repeatedly query the new index.
4) request the new index is removed.

Times for how long each of these requests takes which can give an
idea of the performace of the index server for a particular data set
on a particular machine.
"""

import argparse
import os
import sys
import timeit
import pprint
import random

import grpc

sys.path.append(os.path.dirname(__file__) + '../server_code/protobuf')
import sampling_api_pb2

PARSER = argparse.ArgumentParser(description='Generate random entries for the sample server')
PARSER.add_argument('--server',
                    dest='server',
                    type=str,
                    help='server to connect to',
                    default='localhost')
PARSER.add_argument('--port',
                    dest='port',
                    type=int,
                    help='port to use when connecting to the server',
                    default=40053)
PARSER.add_argument('--input_file',
                    dest='input_file',
                    type=str,
                    help='name of data file',
                    default='')
PARSER.add_argument('--trials',
                    dest='trial_count',
                    type=int,
                    help='Number of trials to run for each test',
                    default=1000)
PARSER.add_argument('--index_name',
                    dest='index_name',
                    type=str,
                    help='Name of test index to be inserted into the sample server',
                    default='python_test_index')

ARGS = PARSER.parse_args()

def list_test(mystub, trials):
    """test the time to request a list of indexes

    mystub - the connection to send list requests across
    trials - number of requests to send

    returns - a dictionary containing statistics of the test
    """
    empty = sampling_api_pb2.ListStructuresRequest()
    start_time = timeit.default_timer()
    for _ in xrange(0, trials):
        indexes = mystub.ListSamplingStructure(empty)
    end_time = timeit.default_timer()

    ret_val = {}
    ret_val['total_time'] = end_time - start_time
    ret_val['trials'] = trials
    ret_val['index count'] = len(indexes.structures)
    ret_val['mean time'] = (end_time - start_time) / trials

    name_list = []
    for index in indexes.structures:
        name_list.append(index.name)

    ret_val['index names'] = name_list

    return ret_val

def build_test(mystub, file_location, index_name):
    """test the time to build an index in the sample server

    mystub - the connection to send the build request across
    file_location - the location of the input data (local to the index server)
    index_name - the name of the index being created

    returns - a dictionary containing statistics of the test.

    After this test runs, the sample server will have an index with
    the appropreate name.
    """
    request = sampling_api_pb2.BuildRequest()
    request.name = index_name
    request.force = True
    request.input_location = file_location
    request.remove_input = False

    start_time = timeit.default_timer()
    mystub.BuildStructure(request)
    end_time = timeit.default_timer()

    ret_val = {}
    ret_val['total time'] = end_time - start_time
    ret_val['index name'] = index_name
    return ret_val

def delete_test(mystub, index_name):
    """tests the time to drop an index in the sample server

    mystub - the connection to send a delete request across
    index_name - name of index to drop

    returns - a dictionary containing statistics of the test

    After this test runs, the sample server will no longer have
    an index with the name provided by index_name
    """
    request = sampling_api_pb2.DropRequest()
    request.structure_name = index_name

    start_time = timeit.default_timer()
    mystub.DropStructure(request)
    end_time = timeit.default_timer()

    ret_val = {}
    ret_val['total time'] = end_time - start_time
    ret_val['index name'] = index_name
    return ret_val

def query_test(mystub, index_name, trials, request_data):
    """Perform a series of random queries on a sample server

    mystub - the connection to send queries across
    index_name - name of index to query
    trials - number of queries to run before returning
    request_data - True if all data should be requested from the sample server

    returns - a dictionary containing statistics of the test

    index bounds for the random queries are created by generating random
    points for latitude and longitude.  The time value is always the same.
    A query is registered with the sample server then a large number
    of samples are requested from the server.  The time to perform these
    actions is individually recorded and returned.
    """
    setup_time = []
    query_time = []
    count_est = []
    area = []

    for _ in xrange(0, trials):
        lat = [random.uniform(-90.0, 90.0), random.uniform(-90.0, 90.0)]
        lon = [random.uniform(-180.0, 180.0), random.uniform(-180.0, 180.0)]
        time = [0, 2147483647]

        start_start_time = timeit.default_timer()

        start_request = sampling_api_pb2.StartQueryRequest()
        start_request.structure_name = index_name
        start_request.query_region.min_point.lat = min(lat)
        start_request.query_region.min_point.lon = min(lon)
        start_request.query_region.min_point.time = int(min(time))
        start_request.query_region.max_point.lat = max(lat)
        start_request.query_region.max_point.lon = max(lon)
        start_request.query_region.max_point.time = int(max(time))
        start_request.return_OID = request_data
        start_request.return_location = request_data
        start_request.return_time = request_data
        start_request.return_payload = request_data
        start_request.suggested_ttl = 10

        start_result = mystub.StartQuery(start_request)

        start_end_time = timeit.default_timer()
        query_start_time = timeit.default_timer()

        query_request = sampling_api_pb2.QueryRequest()
        query_request.query_id = start_result.query_id
        query_request.elements_to_return = 10000

        query_result = mystub.Query(query_request)
        query_end_time = timeit.default_timer()

        setup_time.append(start_end_time - start_start_time)
        query_time.append(query_end_time - query_start_time)

        # the value of 'total_count' should be the same for all elements statistics returned.
        # I just selected one of them to pull out the count value
        count_est.append(query_result.lat_last.total_count)

        # I know the meaning of 'area' is not well defined.  However, it does help
        # give an idea of how much of the region is covered.  Also, the meaning
        # of 'area' is more well defined if data_gen.py is used to generate uniform
        # random input data for this test.
        area.append((max(lat) - min(lat)) * (max(lon) - min(lon)))

    ret_val = {}

    ret_val['total time'] = sum(setup_time) + sum(query_time)

    ret_val['setup time'] = setup_time
    ret_val['max setup time'] = max(setup_time)
    ret_val['min setup time'] = min(setup_time)

    ret_val['query time'] = query_time
    ret_val['max query time'] = max(query_time)
    ret_val['min query time'] = min(query_time)

    ret_val['area of queries'] = area
    ret_val['max query area'] = max(area)
    ret_val['min query area'] = min(area)

    ret_val['index'] = index_name
    ret_val['query trials'] = trials
    ret_val['data returned'] = request_data

    return ret_val


CHANNEL = grpc.insecure_channel(ARGS.server + ':' + str(ARGS.port))
STUB = sampling_api_pb2.SamplingDatabaseStub(CHANNEL)

PP = pprint.PrettyPrinter(indent=4)

print "running list index tests"
R_VAL = list_test(STUB, ARGS.trial_count)
print "query index list results"
PP.pprint(R_VAL)

print "build test"
R_VAL = build_test(STUB, ARGS.input_file, ARGS.index_name)
print "build test results"
PP.pprint(R_VAL)

print "query test"
R_VAL = query_test(STUB, ARGS.index_name, ARGS.trial_count, False)
print "query test results"
PP.pprint(R_VAL)

print "delete test"
R_VAL = delete_test(STUB, ARGS.index_name)
print "delete test results"
PP.pprint(R_VAL)

print "done"

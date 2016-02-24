"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  This file is part of the Smart Developer Hub Project:
    http://www.smartdeveloperhub.org

  Center for Open Middleware
        http://www.centeropenmiddleware.com/
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Copyright (C) 2015 Center for Open Middleware.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at 

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""
import calendar
import json
import logging
import traceback
from datetime import datetime

from agora.scholar.actions import FragmentConsumerResponse
from agora.scholar.daemons.fragment import is_fragment_synced, fragment_graph, fragment_lock
from agora.stoa.actions.core import STOA
from agora.stoa.actions.core.fragment import FragmentRequest, FragmentAction, FragmentSink
from agora.stoa.actions.core.utils import chunks, tp_parts
from shortuuid import uuid

__author__ = 'Fernando Serena'

log = logging.getLogger('agora.scholar.actions.query')


class QueryRequest(FragmentRequest):
    def __init__(self):
        super(QueryRequest, self).__init__()

    def _extract_content(self, request_type=STOA.QueryRequest):
        """
        Parse query request data. For this operation, there is no additional data to extract.
        """
        super(QueryRequest, self)._extract_content(request_type=request_type)


class QueryAction(FragmentAction):
    def __init__(self, message):
        """
        Prepare request and sink objects before starting initialization
        """
        self.__request = QueryRequest()
        self.__sink = QuerySink()
        super(QueryAction, self).__init__(message)

    @property
    def sink(self):
        return self.__sink

    @classmethod
    def response_class(cls):
        return QueryResponse

    @property
    def request(self):
        return self.__request

    def submit(self):
        """
        If the fragment is already synced at submission time, the delivery becomes ready
        """
        super(QueryAction, self).submit()
        if is_fragment_synced(self.sink.fragment_id):
            self.sink.delivery = 'ready'


class QuerySink(FragmentSink):
    """
    Query sink does not need any extra behaviour
    """
    def _remove(self, pipe):
        super(QuerySink, self)._remove(pipe)

    def __init__(self):
        super(QuerySink, self).__init__()

    def _save(self, action):
        super(QuerySink, self)._save(action)

    def _load(self):
        super(QuerySink, self)._load()


class QueryResponse(FragmentConsumerResponse):
    def __init__(self, rid):
        # The creation of a response always require to load its corresponding sink
        self.__sink = QuerySink()
        self.__sink.load(rid)
        super(QueryResponse, self).__init__(rid)
        self.__fragment_lock = fragment_lock(self.__sink.fragment_id)

    @property
    def sink(self):
        return self.__sink

    def _build(self):
        self.__fragment_lock.acquire()
        result = self.query()
        log.debug('Building a query result for request number {}'.format(self._request_id))

        try:
            # All those variables that start with '_' are not projected
            # TODO: improve this way of selecting variables
            variables = filter(lambda x: not x.startswith('_'), map(lambda v: v.lstrip('?'),
                                                                    filter(lambda x: x.startswith('?'),
                                                                           self.sink.preferred_labels)))

            # Query result chunking, yields JSON
            for ch in chunks(result, 100):
                result_rows = []
                for t in ch:
                    if any(t):
                        result_row = {v: t[v] for v in variables}
                        result_rows.append(result_row)
                if result_rows:
                    yield json.dumps(result_rows), {'state': 'streaming', 'source': 'store',
                                                    'response_to': self.sink.message_id,
                                                    'submitted_on': calendar.timegm(datetime.now().timetuple()),
                                                    'submitted_by': self.sink.submitted_by,
                                                    'format': 'json'}
        except Exception, e:
            log.error(e.message)
            raise
        finally:
            self.__fragment_lock.release()
            yield '', {'state': 'end'}

        # Just after sending the state:end message, the request delivery state switches to sent
        self.sink.delivery = 'sent'

    def query(self):
        """
        Query the fragment using the original request graph pattern
        :return: The query result
        """
        def __transform(x):
            """
            Trick to avoid literal language tags problem, etc.
            """
            if x.startswith('"'):
                var = uuid()
                return '?%s FILTER(str(?%s) = %s)' % (var, var, x)
            return x

        def __make_optional(x):
            if 'FILTER' not in x and not x.startswith('<'):
                x = 'OPTIONAL { %s }' % x
            return x

        gp = [' '.join([__transform(self.sink.map(part, fmap=True)) for part in tp_parts(tp)]) for tp in
              self.sink.fragment_gp]
        gp = map(__make_optional, gp)
        where_gp = ' . '.join(gp)

        query = """SELECT %s WHERE { %s }""" % (' '.join(self.sink.preferred_labels), where_gp)

        result = []
        try:
            result = fragment_graph(self.sink.fragment_id).query(query)
        except Exception, e:  # ParseException from query
            traceback.print_exc()
            log.warning(e.message)
        return result

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
from datetime import datetime

from agora.scholar.actions import FragmentConsumerResponse
from agora.scholar.daemons.fragment import is_fragment_synced, fragment_lock
from agora.stoa.actions.core import STOA
from agora.stoa.actions.core.fragment import FragmentRequest, FragmentAction, FragmentSink
from agora.stoa.actions.core.utils import chunks
from agora.stoa.store.tables import db


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

    def _save(self, action, general=True):
        super(QuerySink, self)._save(action, general)

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
        result = self.result_set()
        log.debug('Building a query result for request number {}'.format(self._request_id))

        try:
            # Query result chunking, yields JSON
            for ch in chunks(result, 100):
                result_rows = []
                for t in ch:
                    if any(t):
                        result_row = {self.sink.map('?' + v).lstrip('?'): t[v] for v in t}
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
            yield [], {'state': 'end', 'format': 'json'}

        # Just after sending the state:end message, the request delivery state switches to sent
        self.sink.delivery = 'sent'

    def result_set(self):
        def extract_fields(result):
            for r in result:
                yield r['_id']

        pattern = {}
        projection = {}
        mapping = filter(lambda x: x.startswith('?'), self.sink.mapping)
        for v in mapping:
            value = self.sink.map(v, fmap=True)
            if not value.startswith('?'):
                pattern[v.lstrip('?')] = value.strip('"')
            elif not value.startswith('?_'):
                # All those variables that start with '_' won't be projected
                projection[v.lstrip('?')] = True

        table = db[self.sink.fragment_id]
        pipeline = [{"$match": {v: pattern[v] for v in pattern}},
                    {"$group": {'_id': {v: '$' + v for v in projection}}}]
        return extract_fields(table.aggregate(pipeline))

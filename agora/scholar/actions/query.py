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

from agora.scholar.daemons.fragment import FragmentPlugin, is_fragment_synced, fragment_graph
from agora.stoa.actions.core.fragment import FragmentRequest, FragmentAction, FragmentSink
from agora.scholar.actions import FragmentConsumerResponse
from agora.stoa.actions.core.utils import chunks
from rdflib import Literal
from rdflib.namespace import XSD
from agora.stoa.store.triples import cache

__author__ = 'Fernando Serena'

log = logging.getLogger('agora.scholar.actions.query')


class QueryPlugin(FragmentPlugin):
    @property
    def sink_class(self):
        return QuerySink

    def consume(self, fid, (c, s, p, o), graph, *args):
        pass

    def complete(self, fid, *args):
        pass


FragmentPlugin.register(QueryPlugin)


class QueryRequest(FragmentRequest):
    def __init__(self):
        super(QueryRequest, self).__init__()

    def _extract_content(self):
        super(QueryRequest, self)._extract_content()

        q_res = self._graph.query("""SELECT ?node WHERE {
                                        ?node a stoa:QueryRequest .
                                    }""")

        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid query request')

        request_fields = q_res.pop()
        if not all(request_fields):
            raise ValueError('Missing fields for query request')
        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')


class QueryAction(FragmentAction):
    def __init__(self, message):
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
        try:
            super(QueryAction, self).submit()
        except Exception as e:
            log.debug('Bad request: {}'.format(e.message))
            self._reply_failure(e.message)


class QuerySink(FragmentSink):
    def _remove(self, pipe):
        super(QuerySink, self)._remove(pipe)

    def __init__(self):
        super(QuerySink, self).__init__()

    def _save(self, action):
        super(QuerySink, self)._save(action)
        if is_fragment_synced(self.fragment_id):
            log.debug('Request {} is already backed'.format(self._request_id))
            self.delivery = 'ready'

    def _load(self):
        super(QuerySink, self)._load()


class QueryResponse(FragmentConsumerResponse):
    def __init__(self, rid):
        self.__sink = QuerySink()
        self.__sink.load(rid)
        super(QueryResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def _build(self):
        fragment = self.fragment()
        log.debug('Building a query result for request number {}'.format(self._request_id))

        try:
            variables = filter(lambda x: not x.startswith('_'), map(lambda v: v.lstrip('?'),
                                                                    filter(lambda x: x.startswith('?'),
                                                                           self.sink.preferred_labels)))

            for ch in chunks(fragment, 100):
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
            yield '', {'state': 'end'}
        self.sink.delivery = 'sent'

    def fragment(self):
        def __transform(x):
            if x.startswith('"'):
                return Literal(x.replace('"', ''), datatype=XSD.string).n3(cache.namespace_manager)
            return x

        gp = [' '.join([__transform(self.sink.map(part)) for part in tp.split(' ')]) for tp in self.sink.gp]
        where_gp = ' . '.join(gp)
        # TODO: Consider using selective OPTIONAL clauses
        query = """SELECT %s WHERE { %s }""" % (' '.join(self.sink.preferred_labels), where_gp)

        result = []
        try:
            result = fragment_graph(self.sink.fragment_id).query(query)
        except Exception, e:  # ParseException from query
            log.warning(e.message)
        return result

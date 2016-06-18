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

import networkx as nx
from agora.scholar.actions import FragmentConsumerResponse
from agora.scholar.daemons.fragment import fragment_lock, fragment_graph, fragments_key, fragment_updated_on, \
    FragmentPlugin
from agora.stoa.actions.core import STOA
from agora.stoa.actions.core.fragment import FragmentRequest, FragmentAction, FragmentSink
from agora.stoa.actions.core.utils import chunks, tp_parts
from agora.stoa.store import r
from agora.stoa.store.tables import db

__author__ = 'Fernando Serena'

log = logging.getLogger('agora.scholar.actions.query')


def fragment_has_result_set(fid):
    return r.get('{}:{}:rs'.format(fragments_key, fid)) is not None


def _update_result_set(fid, gp):
    try:
        result_gen = _query(fid, gp)
        # solutions = _build_solutions(fid, gp)
        # for s in solutions:
        #     print s
        removed = db[fid].delete_many({}).deleted_count
        log.info('{} rows removed from fragment {} result set'.format(removed, fid))
        table = db[fid]
        rows = set(result_gen)
        if rows:
            table.insert_many([{label: row[row.labels[label]] for label in row.labels} for row in rows])
        log.info('{} rows inserted into fragment {} result set'.format(len(rows), fid))

        with r.pipeline(transaction=True) as p:
            p.multi()
            p.set('{}:{}:rs'.format(fragments_key, fid), True)
            p.execute()

    except Exception as e:
        traceback.print_exc()
        log.error(e.message)


# def _build_solutions(fid, gp):
    # gp_parts = [tp_parts(tp) for tp in gp]

    # gp_graph = nx.DiGraph()
    # for gp_part in gp_parts:
    #     gp_graph.add_edge(gp_part[0], gp_part[2], predicate=gp_part[1])
    #
    # roots = filter(lambda x: gp_graph.in_degree(x) == 0, gp_graph.nodes())
    #
    # sorted_pairs = []
    # gp_graph.edges()
    # for root in roots:
    #     succs = gp_graph.successors(root)
    #     sort



    # yield fid


def _query(fid, gp):
    """
    Query the fragment using the original request graph pattern
    :param gp:
    :param fid:
    :return: The query result
    """

    def __build_query_from(x, depth=0):
        def build_pattern_query((u, v, data)):
            return '\nOPTIONAL { %s %s %s %s }' % (u, data['predicate'], v, __build_query_from(v, depth + 1))

        out_edges = list(gp_graph.out_edges_iter(x, data=True))
        out_edges = reversed(sorted(out_edges, key=lambda x: gp_graph.out_degree))
        if out_edges:
            return ' '.join([build_pattern_query(x) for x in out_edges])
        return ''

    gp_parts = [tp_parts(tp) for tp in gp]

    blocks = []
    gp_graph = nx.DiGraph()
    for gp_part in gp_parts:
        gp_graph.add_edge(gp_part[0], gp_part[2], predicate=gp_part[1])

    roots = filter(lambda x: gp_graph.in_degree(x) == 0, gp_graph.nodes())

    blocks += ['%s a stoa:Root\nOPTIONAL { %s }' % (root, __build_query_from(root)) for root in roots]

    where_gp = ' .\n'.join(blocks)
    q = """SELECT DISTINCT * WHERE { %s }""" % where_gp

    result = []
    try:
        log.info('Querying fragment {}:\n{}'.format(fid, q))
        result = fragment_graph(fid).query(q)
    except Exception as e:  # ParseException from query
        traceback.print_exc()
        log.warning(e.message)
    return result


class QueryPlugin(FragmentPlugin):
    @property
    def sink_class(self):
        return QuerySink

    def consume(self, fid, quad, graph, *args):
        pass
        # (subj, _, obj) = quad[0]
        # collection_name = '{}:{}:{}:{}'.format(fragments_key, fid, subj, obj)

        # db[collection_name].insert({subj: str(quad[1]), obj: str(quad[3])})

    @property
    def sink_aware(self):
        return False

    def complete(self, fid, *args):
        fragment_gp = args[0]

        try:
            if fragment_has_result_set(fid):
                _update_result_set(fid, fragment_gp)
        except Exception, e:
            traceback.print_exc()
            log.error(e.message)

        # collection_prefix = '{}:{}'.format(fragments_key, fid)
        # for c in filter(lambda x: x.startswith(collection_prefix),
        #                 db.collection_names(include_system_collections=False)):
        #     db.drop_collection(c)
            # collection_name = '{}:{}:{}:{}'.format(fragments_key, fid, subj, obj)
            # # intermediate_fid_keys = r.keys('{}:{}:int*'.format(fragments_key, fid))
            # with r.pipeline() as p:
            #     for ifk in intermediate_fid_keys:
            #         p.delete(ifk)
            #     p.execute()


FragmentPlugin.register(QueryPlugin)


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
        if fragment_updated_on(self.sink.fragment_id) is not None:
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
            for ch in chunks(result, 1000):
                result_rows = []
                for t in ch:
                    if any(t):
                        result_row = {self.sink.map('?' + v).lstrip('?'): t[v] for v in t}
                        result_rows.append(result_row)
                if result_rows:
                    yield json.dumps(result_rows), {'state': 'streaming', 'source': 'store',
                                                    'response_to': self.sink.message_id,
                                                    'submitted_on': calendar.timegm(datetime.utcnow().timetuple()),
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

        if not r.exists('{}:{}:rs'.format(fragments_key, self.sink.fragment_id)):
            _update_result_set(self.sink.fragment_id, self.sink.fragment_gp)

        pattern = {}
        projection = {}
        mapping = filter(lambda x: x.startswith('?'), self.sink.mapping)
        for v in mapping:
            value = self.sink.map(v, fmap=True)
            if not value.startswith('?'):
                if value.startswith('"'):
                    value = value.strip('"')
                else:
                    value = value.lstrip('<').rstrip('>')
                pattern[v.lstrip('?')] = value
            elif not value.startswith('?_'):
                # All those variables that start with '_' won't be projected
                projection[v.lstrip('?')] = True

        table = db[self.sink.fragment_id]
        pipeline = [{"$match": {v: pattern[v] for v in pattern}},
                    {"$group": {'_id': {v: '$' + v for v in projection}}}]
        return extract_fields(table.aggregate(pipeline))

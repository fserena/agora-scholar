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
import logging
import time
import traceback
from datetime import datetime as dt, datetime
from datetime import timedelta
from threading import Thread
from time import sleep

import networkx as nx
from abc import abstractmethod, abstractproperty
from agora.client.namespaces import AGORA
from agora.client.wrapper import Agora
from agora.stoa.actions.core import STOA, AGENT_ID
from agora.stoa.actions.core.fragment import fragments_key, fragment_lock
from agora.stoa.actions.core.utils import tp_parts, GraphPattern
from agora.stoa.daemons.delivery import build_response
from agora.stoa.server import app
from agora.stoa.store import r
from agora.stoa.store.triples import fragments_cache, add_stream_triple, clear_fragment_stream, load_stream_triples, \
    GraphProvider, event_resource_callbacks
from concurrent.futures.thread import ThreadPoolExecutor
from rdflib import RDF, RDFS
from rdflib.term import URIRef

__author__ = 'Fernando Serena'

log = logging.getLogger('agora.scholar.daemons.fragment')

# Load environment variables
agora_client = Agora(**app.config['AGORA'])
ON_DEMAND_TH = float(app.config.get('PARAMS', {}).get('on_demand_threshold', 2.0))
MIN_SYNC = int(app.config.get('PARAMS', {}).get('min_sync_time', 10))
N_COLLECTORS = int(app.config.get('PARAMS', {}).get('fragment_collectors', 1))
MAX_CONCURRENT_FRAGMENTS = int(app.config.get('PARAMS', {}).get('max_concurrent_fragments', 8))
COLLECT_THROTTLING = max(1, int(app.config.get('PARAMS', {}).get('collect_throttling', 30)))
THROTTLING_TIME = (1.0 / (COLLECT_THROTTLING * 1000))

log.info("""Fragment daemon setup:
                    - On-demand threshold: {}
                    - Minimum sync time: {}
                    - Maximum concurrent collectors: {}
                    - Maximum concurrent fragments: {}""".format(ON_DEMAND_TH, MIN_SYNC, N_COLLECTORS,
                                                                 MAX_CONCURRENT_FRAGMENTS))

# Fragment collection threadpool
thp = ThreadPoolExecutor(max_workers=min(8, MAX_CONCURRENT_FRAGMENTS))

# Create a graph provider
graph_provider = GraphProvider()

log.info('Cleaning fragment locks...')
fragment_locks = r.keys('{}:*lock*'.format(fragments_key))
for flk in fragment_locks:
    r.delete(flk)

log.info('Cleaning fragment pulling flags...')
fragment_pullings = r.keys('{}:*:pulling'.format(fragments_key))
for fpk in fragment_pullings:
    r.delete(fpk)

resource_in_fragment = {}
fragment_resources = {}
resource_events = {}


class FragmentPlugin(object):
    """
    Abstract class to be implemented for each action that requires to be notified after each fragment
    collection event, e.g. new triple found
    """

    # Plugins list, all of them will be notified in order
    __plugins = []

    @classmethod
    def register(cls, p):
        """
        Register a fragment plugin (they all should be subclasses of FragmentPlugin)
        :param p: Plugin
        """
        if issubclass(p, cls):
            cls.__plugins.append(p())
        else:
            raise ValueError('{} is not a valid fragment plugin'.format(p))

    @classmethod
    def plugins(cls):
        """
        :return: The list of registered plugins
        """
        return cls.__plugins[:]

    @abstractmethod
    def consume(self, fid, quad, graph, *args):
        """
        This method will be invoked just after a new fragment triple is found
        :param fid: Fragment id
        :param quad: (context, subject, predicate, object)
        :param graph: The search plan graph that 
        :param args: Context arguments, e.g. sink
        :return:
        """
        pass

    @abstractmethod
    def complete(self, fid, *args):
        """
        This method will be invoked just after a fragment is fully collected
        :param fid: Fragment id
        :param args: Context arguments, e.g. sink
        """
        pass

    @abstractproperty
    def sink_class(self):
        """
        The specific Sink class to work with
        """
        pass

    def sink_aware(self):
        return True


def __bind_prefixes(source_graph):
    """
    Binds all source graph prefixes to the cache graph
    """
    map(lambda (prefix, uri): fragments_cache.bind(prefix, uri), source_graph.namespaces())


def match_filter(elm, f):
    """
    Check if a given term is equal to a filter string
    :param elm: The term
    :param f: Filter string
    :return: Boolean value
    """
    if f.startswith('"'):
        return unicode(elm) == f.lstrip('"').rstrip('"')
    elif f.startswith('<'):
        return unicode(elm) == f.lstrip('<').rstrip('>')
    return False


def map_variables(tp, mapping=None, fmap=None):
    """
    Find a mapping for a given triple pattern tuple
    :param tp: (subject, predicate, object) string-based tuple
    :param mapping: Concrete variables' mapping dictionary
    :param fmap: Concrete filter-variable mapping dictionary
    :return: A mapped triple pattern
    """

    def apply_filter_map(x):
        return x if fmap is None else fmap.get(x, x)

    return tp if mapping is None else tuple(map(lambda x: apply_filter_map(mapping.get(x, x)), tp))


def __consume_quad(fid, (c, s, p, o), graph, sinks=None):
    """
    Proxy all new-triple-found fragment events to the registered plugins
    :param fid: Fragment id
    :param graph: Search plan graph
    :param sinks: Fragment requests' sinks
    """

    def __process_filters(sink):
        """
        It's very important to take into account the filter mapping of each request to not
        notify about triples that do not fit with the actual graph pattern of each of them
        :return: Boolean value that determines whether the triple must be sent to a specific sink-aware plugin
        """
        filter_mapping = sink.filter_mapping
        real_context = map_variables(c, sink.mapping, filter_mapping)

        consume = True
        if sink.map(c[2]) in filter_mapping:
            consume = match_filter(o, real_context[2])
        if consume and sink.map(c[0]) in filter_mapping:
            consume = match_filter(s, real_context[0])

        return consume, real_context

    def __sink_consume():
        """
        Function for notifying sink-aware plugins
        """
        for rid in filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks):
            sink = sinks[rid]
            try:
                consume, real_context = __process_filters(sink)
                if consume:
                    plugin.consume(fid, (real_context, s, p, o), graph, sink)
            except Exception as e:
                log.warning(e.message)
                plugin.complete(fid, sink)
                sink.remove()
                yield rid

    def __generic_consume():
        try:
            plugin.consume(fid, (c, s, p, o), graph)
        except Exception as e:
            log.warning(e.message)

    # In case the plugin is not sink-aware, proceed with a generic notification
    for plugin in FragmentPlugin.plugins():
        if plugin.sink_class is not None:
            invalid_sinks = list(__sink_consume())
            for _ in invalid_sinks:
                del sinks[_]
        else:
            __generic_consume()


def __notify_completion(fid, sinks):
    """
    Notify the ending of a fragment collection to all registered plugins
    :param fid: Fragment id
    :param sinks: Set of dependent sinks
    :return:
    """

    for sink in sinks.values():
        if sink.delivery == 'accepted':
            sink.delivery = 'ready'

    if FragmentPlugin.plugins:
        fragment_gp = GraphPattern(r.smembers('{}:fragments:{}:gp'.format(AGENT_ID, fid)))
        for plugin in FragmentPlugin.plugins():
            try:
                filtered_sinks = filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks)
                for rid in filtered_sinks:
                    sink = sinks[rid]
                    if plugin.sink_aware:
                        plugin.complete(fid, sink)
                if not plugin.sink_aware:
                    plugin.complete(fid, fragment_gp)
            except Exception as e:
                log.warning(e.message)


def __extract_tp_from_plan(graph, c):
    """

    :param graph: Search Plan graph
    :param c: Triple pattern node in the search plan
    :return: A string triple representing the pattern for a given search plan triple pattern node
    """

    def extract_node_id(node):
        nid = node
        if (node, RDF.type, AGORA.Variable) in graph:
            nid = list(graph.objects(node, RDFS.label)).pop()
        elif (node, RDF.type, AGORA.Literal) in graph:
            nid = list(graph.objects(node, AGORA.value)).pop()
        return nid

    predicate = list(graph.objects(c, AGORA.predicate)).pop()
    subject_node = list(graph.objects(c, AGORA.subject)).pop()
    object_node = list(graph.objects(c, AGORA.object)).pop()
    subject = extract_node_id(subject_node)
    obj = extract_node_id(object_node)

    return str(subject), predicate.n3(graph.namespace_manager), str(obj)


# Cache is used as the triple store for fragments.
# Each fragment is assigned three different main contexts:
#   - fid: Where all its triple patterns are persisted
#   - /fid: Fragment data
#   - (fid, c): Triple pattern based fragment data (1 context per triple pattern, c)


def graph_from_gp(gp):
    gp_graph = nx.DiGraph()
    gp_parts = [tp_parts(tp) for tp in gp]
    for gp_part in gp_parts:
        gp_graph.add_edge(gp_part[0], gp_part[2], predicate=gp_part[1])
    return gp_graph


def __update_fragment_cache(fid, gp):
    """
    Recreate fragment <fid> cached data and all its data-contexts from the corresponding stream (Redis)
    :param fid:
    :return:
    """
    fragments_cache.remove_context(fragments_cache.get_context('/' + fid))

    gp_graph = graph_from_gp(gp)
    roots = filter(lambda x: gp_graph.in_degree(x) == 0, gp_graph.nodes())

    fragment_triples = load_stream_triples(fid, calendar.timegm(dt.utcnow().timetuple()))
    visited_contexts = set([])
    for c, s, p, o in fragment_triples:
        if c not in visited_contexts:
            fragments_cache.remove_context(fragments_cache.get_context(str((fid, c))))
            visited_contexts.add(c)
        fragments_cache.get_context(str((fid, c))).add((s, p, o))
        fragments_cache.get_context('/' + fid).add((s, p, o))
        if c[0] in roots:
            fragments_cache.get_context('/' + fid).add((s, RDF.type, STOA.Root))
    visited_contexts.clear()
    with r.pipeline() as pipe:
        pipe.delete('{}:{}:stream'.format(fragments_key, fid))
        pipe.execute()


def __cache_plan_context(fid, graph):
    """
    Use <graph> to extract the triple patterns of the current fragment <fid> and replace them as the expected context
    (triple patterns context) in the cache graph
    """
    try:
        fid_context = fragments_cache.get_context(fid)
        fragments_cache.remove_context(fid_context)
        tps = graph.subjects(RDF.type, AGORA.TriplePattern)
        for tp in tps:
            for (s, p, o) in graph.triples((tp, None, None)):
                fid_context.add((s, p, o))
                for t in graph.triples((o, None, None)):
                    fid_context.add(t)
    except Exception as e:
        log.error(e.message)


def __remove_fragment(fid):
    """
    Completely remove a fragment from the system after notifying its known consumers
    :param fid: Fragment identifier
    """
    log.debug('Waiting to remove fragment {}...'.format(fid))
    lock = fragment_lock(fid)
    lock.acquire()

    r_sinks = __load_fragment_requests(fid)
    __notify_completion(fid, r_sinks)
    fragment_keys = r.keys('{}:{}*'.format(fragments_key, fid))
    with r.pipeline(transaction=True) as p:
        p.multi()
        map(lambda k: p.delete(k), fragment_keys)
        p.srem(fragments_key, fid)
        p.execute()

    # Fragment lock key was just implicitly removed, so it's not necessary to release the lock
    # lock.release()
    log.info('Fragment {} has been removed'.format(fid))


def __load_fragment_requests(fid):
    """
    Load all requests and their sinks that are related to a given fragment id
    :param fid: Fragment id
    :return: A dictionary of sinks of all fragment requests
    """
    sinks_ = {}
    fragment_requests_key = '{}:{}:requests'.format(fragments_key, fid)
    for rid in r.smembers(fragment_requests_key):
        try:
            sinks_[rid] = build_response(rid).sink
        except Exception as e:
            log.warning(e.message)
            with r.pipeline(transaction=True) as p:
                p.multi()
                p.srem(fragment_requests_key, rid)
                p.execute()
    return sinks_


def init_fragment_resources(fid):
    if fid in fragment_resources:
        for s in fragment_resources[fid]:
            if s in resource_in_fragment:
                resource_in_fragment[s].remove(fid)
        del fragment_resources[fid]
    fragment_resources[fid] = set([])


def resource_callback(resource):
    resource_events[resource] = datetime.utcnow()


event_resource_callbacks.add(resource_callback)


def change_in_fragment_resource(fid, lapse):
    if lapse is None:
        return True
    last = datetime.utcnow() - timedelta(seconds=lapse * 3)
    if fid in fragment_resources:
        if fragment_resources[fid]:
            return any([str(s) in resource_events and resource_events[str(s)] >= last for s in fragment_resources[fid]])
    return True


def __pull_fragment(fid):
    """
    Pull and replace (if needed) a given fragment
    :param fid: Fragment id
    """

    fragment_key = '{}:{}'.format(fragments_key, fid)
    on_events = r.get('{}:events'.format(fragment_key))

    if on_events == 'True' and not change_in_fragment_resource(fid, int(r.get('{}:ud'.format(fragment_key)))):
        with r.pipeline(transaction=True) as p:
            p.multi()
            sync_key = '{}:sync'.format(fragment_key)
            p.set(sync_key, True)
            durability = int(r.get('{}:ud'.format(fragment_key)))
            p.expire(sync_key, durability)
            p.set('{}:updated'.format(fragment_key), calendar.timegm(dt.utcnow().timetuple()))
            p.delete('{}:pulling'.format(fragment_key))
            p.execute()
        return

    # Load fragment graph pattern
    tps = r.smembers('{}:gp'.format(fragment_key))
    # Load fragment requests (including their sinks)
    r_sinks = __load_fragment_requests(fid)
    log.info("""Starting collection of fragment {}:
                    - GP: {}
                    - Supporting: ({}) {}""".format(fid, list(tps), len(r_sinks), list(r_sinks)))

    init_fragment_resources(fid)

    # Prepare the corresponding fragment generator and fetch the search plan
    start_time = datetime.utcnow()
    try:
        fgm_gen, _, graph = agora_client.get_fragment_generator('{ %s }' % ' . '.join(tps), workers=N_COLLECTORS,
                                                                provider=graph_provider, queue_size=N_COLLECTORS*100)

    except Exception:
        traceback.print_exc()
        log.error('Agora is not available')
        return

    # In case there is not SearchTree in the plan: notify, remove and abort collection
    if not list(graph.subjects(RDF.type, AGORA.SearchTree)):
        log.info('There is no search plan for fragment {}. Removing...'.format(fid))
        # TODO: Send additional headers notifying the reason to end
        __notify_completion(fid, r_sinks)
        __remove_fragment(fid)
        return

    # Update cache graph prefixes
    __bind_prefixes(graph)

    # Extract triple patterns' dictionary from the search plan
    context_tp = {tpn: __extract_tp_from_plan(graph, tpn) for tpn in
                  graph.subjects(RDF.type, AGORA.TriplePattern)}
    frag_contexts = {tpn: (fid, context_tp[tpn]) for tpn in context_tp}

    lock = fragment_lock(fid)
    lock.acquire()

    # Update fragment contexts
    with r.pipeline(transaction=True) as p:
        p.multi()
        p.set('{}:pulling'.format(fragment_key), True)
        contexts_key = '{}:contexts'.format(fragment_key)
        p.delete(contexts_key)
        clear_fragment_stream(fid)
        for tpn in context_tp.keys():
            p.sadd(contexts_key, frag_contexts[tpn])
        p.execute()
    lock.release()

    # Init fragment collection counters
    n_triples = 0
    fragment_weight = 0
    fragment_delta = 0

    log.info('Collecting fragment {}...'.format(fid))
    try:
        # Iterate all fragment triples and their contexts
        pre_ts = datetime.utcnow()
        for (c, s, p, o) in fgm_gen:
            # Update weights and counters
            triple_weight = len(u'{}{}{}'.format(s, p, o))
            fragment_weight += triple_weight
            fragment_delta += triple_weight

            # Store the triple if it was not obtained before and notify related requests
            try:
                lock.acquire()
                new_triple = add_stream_triple(fid, context_tp[c], (s, p, o))
                lock.release()
                if new_triple:
                    if isinstance(s, URIRef):
                        if s not in resource_in_fragment:
                            resource_in_fragment[s] = set([])
                        resource_in_fragment[s].add(fid)
                        fragment_resources[fid].add(s)
                    __consume_quad(fid, (context_tp[c], s, p, o), graph, sinks=r_sinks)
                n_triples += 1
            except Exception as e:
                log.warning(e.message)
                traceback.print_exc()

            if fragment_delta > 10000:
                fragment_delta = 0
                log.info('Pulling fragment {} [{} kB]'.format(fid, fragment_weight / 1000.0))

            if n_triples % 100 == 0:
                # Update fragment requests
                if r.scard('{}:requests'.format(fragment_key)) != len(r_sinks):
                    r_sinks = __load_fragment_requests(fid)

            post_ts = datetime.utcnow()
            elapsed = (post_ts - pre_ts).total_seconds()
            throttling = THROTTLING_TIME - elapsed
            if throttling > 0:
                sleep(throttling)
            pre_ts = datetime.utcnow()
    except Exception as e:
        log.warning(e.message)
        traceback.print_exc()

    elapsed = (datetime.utcnow() - start_time).total_seconds()
    log.info(
        '{} triples retrieved for fragment {} in {} s [{} kB]'.format(n_triples, fid, elapsed,
                                                                      fragment_weight / 1000.0))

    # Update fragment cache and its contexts
    lock.acquire()
    try:
        __update_fragment_cache(fid, tps)
        log.info('Fragment {} data has been replaced with the recently collected'.format(fid))
        __cache_plan_context(fid, graph)
        log.info('BGP context of fragment {} has been cached'.format(fid))
        log.info('Updating result set for fragment {}...'.format(fid))

        # Calculate sync times and update fragment flags
        with r.pipeline(transaction=True) as p:
            p.multi()
            sync_key = '{}:sync'.format(fragment_key)
            demand_key = '{}:on_demand'.format(fragment_key)
            # Fragment is now synced
            p.set(sync_key, True)
            # If the fragment collection time has not exceeded the threshold, switch to on-demand mode
            # if elapsed < ON_DEMAND_TH and elapsed * random.random() < ON_DEMAND_TH / 4:
            #     p.set(demand_key, True)
            #     log.info('Fragment {} has been switched to on-demand mode'.format(fid))
            # else:
            p.delete(demand_key)

            updated_delay = int(r.get('{}:ud'.format(fragment_key)))
            last_requests_ts = map(lambda x: int(x), r.lrange('{}:hist'.format(fragment_key), 0, -1))
            print last_requests_ts
            current_ts = calendar.timegm(datetime.utcnow().timetuple())
            first_collection = r.get('{}:updated'.format(fragment_key)) is None
            base_ts = last_requests_ts[:]
            if not first_collection:
                if current_ts - base_ts[0] <= updated_delay:
                    current_ts += updated_delay  # Force
                base_ts = [current_ts] + base_ts
            request_intervals = [i - j for i, j in zip(base_ts[:-1], base_ts[1:])]
            if request_intervals:
                avg_gap = reduce(lambda x, y: x + y, request_intervals) / len(request_intervals)
                print avg_gap,
                durability = avg_gap - elapsed if avg_gap > updated_delay else updated_delay - elapsed
            else:
                durability = updated_delay - elapsed

            durability = int(max(durability, 1))
            print durability
            if durability <= updated_delay - elapsed:
                p.expire(sync_key, durability)
                log.info('Fragment {} is considered synced for {} s'.format(fid, durability))
            else:
                clear_fragment_stream(fid)
                p.delete('{}:updated'.format(fragment_key))
                p.delete('{}:hist'.format(fragment_key))
                log.info('Fragment {} will no longer be automatically updated'.format(fid))

            p.set('{}:updated'.format(fragment_key), calendar.timegm(dt.utcnow().timetuple()))
            p.delete('{}:pulling'.format(fragment_key))
            p.execute()

        __notify_completion(fid, r_sinks)
    finally:
        lock.release()

    log.info('Fragment {} collection is complete!'.format(fid))


def __collect_fragments():
    registered_fragments = r.scard(fragments_key)
    synced_fragments = len(r.keys('{}:*:sync'.format(fragments_key)))
    log.info("""Collector daemon started:
                    - Fragments: {}
                    - Synced: {}""".format(registered_fragments, synced_fragments))

    futures = {}
    while True:
        try:
            for fid in filter(
                    lambda x: r.get('{}:{}:sync'.format(fragments_key, x)) is None and r.get(
                        '{}:{}:pulling'.format(fragments_key, x)) is None,
                    r.smembers(fragments_key)):
                if fid in futures:
                    if futures[fid].done():
                        del futures[fid]
                if fid not in futures:
                    futures[fid] = thp.submit(__pull_fragment, fid)
        except Exception as e:
            log.error(e.message)
        finally:
            time.sleep(0.1)


def fragment_updated_on(fid):
    return r.get('{}:{}:updated'.format(fragments_key, fid))


def fragment_on_demand(fid):
    return r.get('{}:{}:on_demand'.format(fragments_key, fid))


def is_pulling(fid):
    return r.get('{}:{}:pulling'.format(fragments_key, fid)) is not None


def fragment_contexts(fid):
    return r.smembers('{}:{}:contexts'.format(fragments_key, fid))


def is_fragment_synced(fid):
    return fragment_updated_on(fid) is not None


def fragment_graph(fid):
    return fragments_cache.get_context('/' + fid)


# Create and start collector daemon
th = Thread(target=__collect_fragments)
th.daemon = True
th.start()

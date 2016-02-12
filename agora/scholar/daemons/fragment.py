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
import random
import time
import traceback
from datetime import datetime as dt, datetime
from threading import Thread
from time import sleep

from redis.lock import Lock

from abc import abstractmethod, abstractproperty
from agora.client.namespaces import AGORA
from agora.client.wrapper import Agora
from agora.stoa.store.triples import cache, add_stream_triple, load_stream_triples, graph_provider
from agora.stoa.daemons.delivery import build_response
from agora.stoa.server import app
from agora.stoa.store import r
from concurrent.futures.thread import ThreadPoolExecutor
from rdflib import RDF, RDFS

__author__ = 'Fernando Serena'

log = logging.getLogger('agora.scholar.daemons.fragment')
agora_client = Agora(**app.config['AGORA'])
ON_DEMAND_TH = float(app.config.get('PARAMS', {}).get('on_demand_threshold', 2.0))
MIN_SYNC = int(app.config.get('PARAMS', {}).get('min_sync_time', 10))
N_COLLECTORS = int(app.config.get('PARAMS', {}).get('fragment_collectors', 1))
MAX_CONCURRENT_FRAGMENTS = int(app.config.get('PARAMS', {}).get('max_concurrent_fragments', 8))
COLLECT_THROTTLING = max(1, int(app.config.get('PARAMS', {}).get('collect_throttling', 30)))

log.info("""Fragment daemon setup:
                    - On-demand threshold: {}
                    - Minimum sync time: {}
                    - Maximum concurrent collectors: {}
                    - Maximum concurrent fragments: {}""".format(ON_DEMAND_TH, MIN_SYNC, N_COLLECTORS,
                                                                 MAX_CONCURRENT_FRAGMENTS))

thp = ThreadPoolExecutor(max_workers=min(8, MAX_CONCURRENT_FRAGMENTS))

log.info('Cleaning fragment locks...')
fragment_locks = r.keys('*lock*')
for flk in fragment_locks:
    r.delete(flk)

log.info('Cleaning fragment pulling flags...')
fragment_pullings = r.keys('fragments:*:pulling')
for fpk in fragment_pullings:
    r.delete(fpk)

log.info('Releasing registered fragments...')
fragment_consumers = r.keys('fragments:*:consumers')
for fck in fragment_consumers:
    r.delete(fck)


def fragment_consumer_lock(fid):
    lock_consume_key = 'fragments:{}:lock:consume'.format(fid)
    return r.lock(lock_consume_key, lock_class=Lock)


def _fragment_lock(fid):
    lock_key = 'fragments:{}:lock'.format(fid)
    return r.lock(lock_key, lock_class=Lock)


class FragmentPlugin(object):
    __plugins = []

    @abstractmethod
    def consume(self, fid, quad, graph, *args):
        pass

    @abstractmethod
    def complete(self, fid, *args):
        pass

    @abstractproperty
    def sink_class(self):
        pass

    def sink_aware(self):
        return True

    @classmethod
    def register(cls, p):
        if issubclass(p, cls):
            cls.__plugins.append(p())
        else:
            raise ValueError('{} is not a valid fragment plugin'.format(p))

    @classmethod
    def plugins(cls):
        return cls.__plugins[:]


def __bind_prefixes(source_graph):
    map(lambda (prefix, uri): cache.bind(prefix, uri), source_graph.namespaces())


def map_variables(tp, mapping):
    if mapping is None:
        return tp
    return tuple(map(lambda x: mapping.get(x, x), tp))


def __consume_quad(fid, (c, s, p, o), graph, sinks=None):
    def __sink_consume():
        for rid in filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks):
            sink = sinks[rid]
            try:
                plugin.consume(fid, (map_variables(c, sink.mapping), s, p, o), graph, sink)
            except Exception as e:
                sink.remove()
                yield rid
                log.warning(e.message)

    def __generic_consume():
        try:
            plugin.consume(fid, (c, s, p, o), graph)
        except Exception as e:
            log.warning(e.message)

    for plugin in FragmentPlugin.plugins():
        if plugin.sink_class is not None:
            invalid_sinks = list(__sink_consume())
            for _ in invalid_sinks:
                del sinks[_]
        else:
            __generic_consume()


def __notify_completion(fid, sinks):
    for plugin in FragmentPlugin.plugins():
        try:
            filtered_sinks = filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks)
            for rid in filtered_sinks:
                sink = sinks[rid]
                if sink.delivery == 'accepted':
                    sink.delivery = 'ready'
                if plugin.sink_aware:
                    plugin.complete(fid, sink)
            if not plugin.sink_aware:
                plugin.complete(fid)
        except Exception as e:
            log.warning(e.message)


def __triple_pattern(graph, c):
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


def __replace_fragment(fid):
    """
    Recreate fragment <fid> cached data and all its data-contexts from the corresponding stream (Redis)
    :param fid:
    :return:
    """
    tps = cache.get_context(fid).subjects(RDF.type, AGORA.TriplePattern)
    cache.remove_context(cache.get_context('/' + fid))
    for tp in tps:
        cache.remove_context(cache.get_context(str((fid, __triple_pattern(cache, tp)))))
    fragment_triples = load_stream_triples(fid, calendar.timegm(dt.now().timetuple()))
    for c, s, p, o in fragment_triples:
        cache.get_context(str((fid, c))).add((s, p, o))
        cache.get_context('/' + fid).add((s, p, o))
    with r.pipeline() as pipe:
        pipe.delete('fragments:{}:stream'.format(fid))
        pipe.execute()


def __cache_plan_context(fid, graph):
    """
    Use <graph> to extract the triple patterns of the current fragment <fid> and replace them as the expected context
    (triple patterns context) in the cache graph
    :param fid:
    :param graph:
    :return:
    """
    try:
        fid_context = cache.get_context(fid)
        cache.remove_context(fid_context)
        tps = graph.subjects(RDF.type, AGORA.TriplePattern)
        for tp in tps:
            for (s, p, o) in graph.triples((tp, None, None)):
                fid_context.add((s, p, o))
                for t in graph.triples((o, None, None)):
                    fid_context.add(t)
    except Exception, e:
        log.error(e.message)


def __remove_fragment(fid):
    log.debug('Waiting to remove fragment {}...'.format(fid))
    lock = _fragment_lock(fid)
    lock.acquire()

    with r.pipeline(transaction=True) as p:
        requests, r_sinks = __load_fragment_requests(fid)
        __notify_completion(fid, r_sinks)
        fragment_keys = r.keys('fragments:{}*'.format(fid))
        map(lambda k: p.delete(k), fragment_keys)
        p.srem('fragments', fid)
        p.execute()

    log.info('Fragment {} has been removed'.format(fid))


def __load_fragment_requests(fid):
    requests_ = r.smembers('fragments:{}:requests'.format(fid))
    sinks_ = {}
    for rid in requests_:
        try:
            sinks_[rid] = build_response(rid).sink
        except Exception, e:
            traceback.print_exc()
            log.warning(e.message)
            with r.pipeline(transaction=True) as p:
                p.multi()
                p.srem('fragments:{}:requests'.format(fid), rid)
                p.execute()
    return requests_, sinks_


def __pull_fragment(fid):
    tps = r.smembers('fragments:{}:gp'.format(fid))
    requests, r_sinks = __load_fragment_requests(fid)
    log.info("""Starting collection of fragment {}:
                    - GP: {}
                    - Supporting: ({}) {}""".format(fid, list(tps), len(requests), list(requests)))
    start_time = datetime.now()

    try:
        fgm_gen, _, graph = agora_client.get_fragment_generator('{ %s }' % ' . '.join(tps), workers=N_COLLECTORS,
                                                                provider=graph_provider, queue_size=N_COLLECTORS)
    except Exception:
        log.error('Agora is not available')
        return

    # There is no search plan to execute
    if not list(graph.subjects(RDF.type, AGORA.SearchTree)):
        log.info('There is no search plan for fragment {}. Removing...'.format(fid))
        # TODO: Send additional headers notifying the reason to end
        __notify_completion(fid, r_sinks)
        __remove_fragment(fid)
        return

    triple_patterns = {tpn: __triple_pattern(graph, tpn) for tpn in
                       graph.subjects(RDF.type, AGORA.TriplePattern)}
    fragment_contexts = {tpn: (fid, triple_patterns[tpn]) for tpn in triple_patterns}
    __bind_prefixes(graph)

    lock = _fragment_lock(fid)
    lock.acquire()

    c_lock = fragment_consumer_lock(fid)
    c_lock.acquire()

    # Update fragment contexts
    with r.pipeline(transaction=True) as p:
        p.multi()
        p.set('fragments:{}:pulling'.format(fid), True)
        p.delete('fragments:{}:contexts'.format(fid))
        for tpn in fragment_contexts.keys():
            p.sadd('fragments:{}:contexts'.format(fid), fragment_contexts[tpn])
        p.execute()
    lock.release()

    c_lock.release()

    n_triples = 0
    fragment_weight = 0
    fragment_delta = 0

    try:
        for (c, s, p, o) in fgm_gen:
            pre_ts = datetime.now()
            triple_weight = len(u'{}{}{}'.format(s, p, o))
            fragment_weight += triple_weight
            fragment_delta += triple_weight
            lock.acquire()
            if add_stream_triple(fid, triple_patterns[c], (s, p, o)):
                __consume_quad(fid, (triple_patterns[c], s, p, o), graph, sinks=r_sinks)
            lock.release()
            if fragment_delta > 1000:
                fragment_delta = 0
                log.info('Pulling fragment {} [{} kB]'.format(fid, fragment_weight / 1000.0))

            if r.scard('fragments:{}:requests'.format(fid)) != len(requests):
                requests, r_sinks = __load_fragment_requests(fid)
            n_triples += 1
            post_ts = datetime.now()
            elapsed = (post_ts - pre_ts).total_seconds()
            excess = (1.0 / COLLECT_THROTTLING) - elapsed
            if excess > 0:
                sleep(excess)
    except Exception, e:
        traceback.print_exc()

    elapsed = (datetime.now() - start_time).total_seconds()
    log.info(
        '{} triples retrieved for fragment {} in {} s [{} kB]'.format(n_triples, fid, elapsed,
                                                                      fragment_weight / 1000.0))

    lock.acquire()
    c_lock.acquire()
    __replace_fragment(fid)
    log.info('Fragment {} data has been replaced with the recently collected'.format(fid))
    __cache_plan_context(fid, graph)
    log.info('BGP context of fragment {} has been cached'.format(fid))
    with r.pipeline(transaction=True) as p:
        p.multi()
        sync_key = 'fragments:{}:sync'.format(fid)
        demand_key = 'fragments:{}:on_demand'.format(fid)
        # Fragment is now synced
        p.set(sync_key, True)
        # If the fragment collection time has not exceeded the threshold, switch to on-demand mode
        if elapsed < ON_DEMAND_TH and elapsed * random.random() < ON_DEMAND_TH / 4:
            p.set(demand_key, True)
            log.info('Fragment {} has been switched to on-demand mode'.format(fid))
        else:
            p.delete(demand_key)
            min_durability = int(max(MIN_SYNC, elapsed))
            durability = random.randint(min_durability, min_durability * 2)
            p.expire(sync_key, durability)
            log.info('Fragment {} is considered synced for {} s'.format(fid, durability))
        p.set('fragments:{}:updated'.format(fid), dt.now())
        p.delete('fragments:{}:pulling'.format(fid))
        p.execute()
    c_lock.release()
    __notify_completion(fid, r_sinks)
    lock.release()


def __collect_fragments():
    registered_fragments = r.scard('fragments')
    synced_fragments = len(r.keys('fragments:*:sync'))
    log.info("""Collector daemon started:
                    - Fragments: {}
                    - Synced: {}""".format(registered_fragments, synced_fragments))

    futures = {}
    while True:
        for fid in filter(
                lambda x: r.get('fragments:{}:sync'.format(x)) is None and r.get(
                    'fragments:{}:pulling'.format(x)) is None,
                r.smembers('fragments')):
            if fid in futures:
                if futures[fid].done():
                    del futures[fid]
            if fid not in futures:
                futures[fid] = thp.submit(__pull_fragment, fid)
        time.sleep(1)


def fragment_updated_on(fid):
    return r.get('fragments:{}:updated'.format(fid))


def fragment_on_demand(fid):
    return r.get('fragments:{}:on_demand'.format(fid))


def is_pulling(fid):
    return r.get('fragments:{}:pulling'.format(fid)) is not None


def fragment_contexts(fid):
    return r.smembers('fragments:{}:contexts'.format(fid))


def is_fragment_synced(fid):
    return fragment_updated_on(fid) is not None


def fragment_graph(fid):
    return cache.get_context('/' + fid)

th = Thread(target=__collect_fragments)
th.daemon = True
th.start()

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
from datetime import datetime as dt, datetime

from redis.lock import Lock

from agora.scholar.actions import FragmentConsumerResponse
from agora.scholar.daemons.fragment import FragmentPlugin, map_variables, match_filter, is_fragment_synced, \
    fragment_contexts
from agora.scholar.daemons.fragment import fragment_lock
from agora.stoa.actions.core import AGENT_ID
from agora.stoa.actions.core import STOA
from agora.stoa.actions.core.fragment import FragmentRequest, FragmentAction, FragmentSink
from agora.stoa.actions.core.utils import parse_bool, chunks
from agora.stoa.messaging.reply import reply
from agora.stoa.store import r
from agora.stoa.store.triples import load_stream_triples, fragments_cache

__author__ = 'Fernando Serena'

log = logging.getLogger('agora.scholar.actions.stream')

log.info("'Cleaning stream requests' locks...")
request_locks = r.keys('{}:requests:*:lock'.format(AGENT_ID))
for rlk in request_locks:
    r.delete(rlk)


class StreamPlugin(FragmentPlugin):
    @property
    def sink_class(self):
        return StreamSink

    def consume(self, fid, (c, s, p, o), graph, *args):
        sink = args[0]
        sink.lock.acquire()
        try:
            # Prevent from consuming a triple when the delivery state says it was completely sent
            # Anyway, this HAS TO BE REMOVED from here, because the stream flag should be enough
            if sink.delivery == 'sent':
                return

            # Proceed only if the stream flag is enabled
            if sink.stream:
                # log.info('[{}] Streaming fragment triple...'.format(sink.request_id))
                reply((c, s.n3(), p.n3(), o.n3()), headers={'source': 'stream', 'format': 'tuple', 'state': 'streaming',
                                                            'response_to': sink.message_id,
                                                            'submitted_on': calendar.timegm(datetime.utcnow().timetuple()),
                                                            'submitted_by': sink.submitted_by},
                      **sink.recipient)
        finally:
            sink.lock.release()

    def complete(self, fid, *args):
        sink = args[0]

        sink.lock.acquire()
        try:
            # At this point, the stream flag is disabled, and the delivery state might need to be updated
            sink.stream = False
            if sink.delivery == 'streaming':
                log.debug('Sending end stream signal after {}'.format(sink.delivery))
                sink.delivery = 'sent'
                reply((), headers={'state': 'end', 'format': 'tuple'}, **sink.recipient)
                log.info('Stream of fragment {} for request {} is done'.format(fid, sink.request_id))
        finally:
            sink.lock.release()


FragmentPlugin.register(StreamPlugin)


class StreamRequest(FragmentRequest):
    def __init__(self):
        super(StreamRequest, self).__init__()

    def _extract_content(self, request_type=STOA.StreamRequest):
        """
        Parse streaming request data. For this operation, there is no additional data to extract.
        """
        super(StreamRequest, self)._extract_content(request_type=request_type)


class StreamAction(FragmentAction):
    def __init__(self, message):
        """
        Prepare request and sink objects before starting initialization
        """
        self.__request = StreamRequest()
        self.__sink = StreamSink()
        super(StreamAction, self).__init__(message)

    @property
    def sink(self):
        return self.__sink

    @classmethod
    def response_class(cls):
        return StreamResponse

    @property
    def request(self):
        return self.__request

    def submit(self):
        super(StreamAction, self).submit()
        # A stream request is ready just after its submission
        self.sink.delivery = 'ready'


class StreamSink(FragmentSink):
    """
    Extends FragmentSink by adding a new property that helps to manage the stream state
    """

    def _remove(self, pipe):
        try:
            self.lock.acquire()
            super(StreamSink, self)._remove(pipe)
            pipe.delete('{}lock'.format(self._request_key))
        except Exception as e:
            log.warning(e.message)

    def __init__(self):
        super(StreamSink, self).__init__()
        self.__lock = None

    def _save(self, action, general=True):
        super(StreamSink, self)._save(action, general)

    def _load(self):
        super(StreamSink, self)._load()
        # Create the request lock
        lock_key = '{}lock'.format(self._request_key)
        self.__lock = r.lock(lock_key, lock_class=Lock)

    @property
    def stream(self):
        return parse_bool(r.hget('{}'.format(self._request_key), '__stream'))

    @stream.setter
    def stream(self, value):
        with r.pipeline(transaction=True) as p:
            p.multi()
            p.hset('{}'.format(self._request_key), '__stream', value)
            p.execute()
        log.info('Request {} stream state is now "{}"'.format(self._request_id, value))

    @property
    def lock(self):
        """
        Helps to manage request stream and delivery status from both plugin events and build response times
        :return: A redis-based lock object for a given request
        """
        return self.__lock


class StreamResponse(FragmentConsumerResponse):
    def __init__(self, rid):
        self.__sink = StreamSink()
        self.__sink.load(rid)
        self.__fragment_lock = fragment_lock(self.__sink.fragment_id)
        super(StreamResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def _build(self):
        """
        This function yields nothing only when the new state is 'streaming'
        :return: Quads like (context, subject, predicate, object)
        """

        timestamp = calendar.timegm(dt.utcnow().timetuple())
        fragment = None

        self.sink.lock.acquire()
        try:
            fragment, streaming = self.fragment(timestamp=timestamp)
            if streaming:
                self.sink.stream = True
                if fragment:
                    self.sink.delivery = 'mixing'
                else:
                    self.sink.delivery = 'streaming'
            else:
                self.sink.stream = False
                if fragment:
                    self.sink.delivery = 'pushing'
                    log.debug('Fragment retrieved from cache for request number {}'.format(self._request_id))
                else:
                    self.sink.delivery = 'sent'
                    log.debug('Sending end stream signal since there is no fragment and stream is disabled')
                    yield (), {'state': 'end', 'format': 'tuple'}
        except Exception as e:
            log.warning(e.message)
            self.sink.stream = True
            self.sink.delivery = 'streaming'
        finally:
            self.sink.lock.release()

        if fragment:
            log.info('Building a stream result from cache for request number {}...'.format(self._request_id))
            filter_mapping = self.sink.filter_mapping
            self.__fragment_lock.acquire()
            try:
                for ch in chunks(fragment, 1000):
                    if ch:
                        rows = []
                        for (c, s, p, o) in ch:
                            real_context = map_variables(c, self.sink.mapping, filter_mapping)
                            consume = True
                            if self.sink.map(c[2]) in filter_mapping:
                                consume = match_filter(o, real_context[2])
                            if consume and self.sink.map(c[0]) in filter_mapping:
                                consume = match_filter(s, real_context[0])
                            if consume:
                                rows.append((real_context, s.n3(), p.n3(), o.n3()))
                        yield rows, {'source': 'store', 'format': 'tuple',
                                     'state': 'streaming',
                                     'response_to': self.sink.message_id,
                                     'submitted_on': calendar.timegm(
                                         datetime.utcnow().timetuple()),
                                     'submitted_by': self.sink.submitted_by}
            finally:
                self.__fragment_lock.release()

        self.sink.lock.acquire()
        try:
            if self.sink.delivery == 'pushing' or (self.sink.delivery == 'mixing' and not self.sink.stream):
                self.sink.delivery = 'sent'
                log.info(
                    'The response stream of request {} is completed. Notifying...'.format(self.sink.request_id))
                yield (), {'state': 'end', 'format': 'tuple'}
            elif self.sink.delivery == 'mixing' and self.sink.stream:
                self.sink.delivery = 'streaming'
        finally:
            self.sink.lock.release()

    def fragment(self, timestamp):
        def __load_contexts():
            contexts = fragment_contexts(self.sink.fragment_id)
            triple_patterns = {context: eval(context)[1] for context in contexts}
            # Yield triples for each known triple pattern context
            for context in contexts:
                for (s, p, o) in fragments_cache.get_context(context):
                    yield triple_patterns[context], s, p, o

        if timestamp is None:
            timestamp = calendar.timegm(dt.utcnow().timetuple())

        self.__fragment_lock.acquire()
        try:
            from_streaming = not is_fragment_synced(self.sink.fragment_id)

            return (load_stream_triples(self.sink.fragment_id, timestamp), True) if from_streaming else (
                __load_contexts(), False)
        finally:
            self.__fragment_lock.release()

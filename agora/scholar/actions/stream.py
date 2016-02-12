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
from agora.scholar.daemons.fragment import FragmentPlugin, map_variables, is_fragment_synced, fragment_contexts
from agora.stoa.actions.core.fragment import FragmentRequest, FragmentAction, FragmentSink
from agora.stoa.actions.core.utils import parse_bool, chunks
from agora.stoa.messaging.reply import reply
from agora.stoa.store import r
from agora.stoa.store.triples import load_stream_triples, cache

__author__ = 'Fernando Serena'

log = logging.getLogger('agora.scholar.actions.stream')


class StreamPlugin(FragmentPlugin):
    @property
    def sink_class(self):
        return StreamSink

    def consume(self, fid, (c, s, p, o), graph, *args):
        sink = args[0]
        if sink.delivery == 'sent':
            return
        if sink.stream:
            log.debug('[{}] Streaming fragment triple...'.format(sink.request_id))
            reply((c, s.n3(), p.n3(), o.n3()), headers={'source': 'stream', 'format': 'tuple', 'state': 'streaming',
                                                        'response_to': sink.message_id,
                                                        'submitted_on': calendar.timegm(datetime.now().timetuple()),
                                                        'submitted_by': sink.submitted_by},
                  **sink.recipient)

    def complete(self, fid, *args):
        sink = args[0]
        sink.stream = False
        if sink.delivery == 'streaming':
            log.debug('Sending end stream signal after {}'.format(sink.delivery))
            sink.delivery = 'sent'
            reply((), headers={'state': 'end'}, **sink.recipient)
            log.info('Stream of fragment {} for request {} is done'.format(fid, sink.request_id))


FragmentPlugin.register(StreamPlugin)


class StreamRequest(FragmentRequest):
    def __init__(self):
        super(StreamRequest, self).__init__()

    def _extract_content(self):
        super(StreamRequest, self)._extract_content()

        q_res = self._graph.query("""SELECT ?node WHERE {
                                        ?node a stoa:StreamRequest .
                                    }""")

        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid query request')

        request_fields = q_res.pop()
        if not all(request_fields):
            raise ValueError('Missing fields for stream request')
        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')


class StreamAction(FragmentAction):
    def __init__(self, message):
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
        try:
            super(StreamAction, self).submit()
        except Exception as e:
            log.debug('Bad request: {}'.format(e.message))
            self._reply_failure(e.message)


class StreamSink(FragmentSink):
    def _remove(self, pipe):
        super(StreamSink, self)._remove(pipe)

    def __init__(self):
        super(StreamSink, self).__init__()

    def _save(self, action):
        super(StreamSink, self)._save(action)
        self.delivery = 'ready'

    def _load(self):
        super(StreamSink, self)._load()

    @property
    def stream(self):
        return parse_bool(r.hget('requests:{}'.format(self._request_id), '__stream'))

    @stream.setter
    def stream(self, value):
        with r.pipeline(transaction=True) as p:
            p.multi()
            p.hset('requests:{}'.format(self._request_id), '__stream', value)
            p.execute()
        log.info('Request {} stream state is now "{}"'.format(self._request_id, value))


class StreamResponse(FragmentConsumerResponse):
    def __init__(self, rid):
        self.__sink = StreamSink()
        self.__sink.load(rid)
        super(StreamResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def _build(self):
        """
        This function does not yield anything only when the new state is 'streaming'
        :return:
        """

        timestamp = calendar.timegm(dt.now().timetuple())
        lock = r.lock('fragments:{}:lock'.format(self.sink.fragment_id), lock_class=Lock)
        lock.acquire()
        fragment = None
        try:
            fragment, stream = self.fragment(timestamp=timestamp)
            if stream:
                self.sink.stream = True
                if fragment:
                    self.sink.delivery = 'mixing'
                else:
                    self.sink.delivery = 'streaming'
            else:
                if fragment:
                    self.sink.delivery = 'pushing'
                    log.debug('Fragment retrieved from cache for request number {}'.format(self._request_id))
                else:
                    self.sink.delivery = 'sent'
                    log.debug('Sending end stream signal since there is no fragment and stream is disabled')
                    yield (), {'state': 'end'}
                self.sink.stream = False
        except Exception as e:
            log.warning(e.message)
            self.sink.stream = True
            self.sink.delivery = 'streaming'
        finally:
            lock.release()

        if fragment:
            log.info('Building a stream result from cache for request number {}...'.format(self._request_id))
            for ch in chunks(fragment, 1000):
                if ch:
                    yield [(map_variables(c, self.sink.mapping), s.n3(), p.n3(), o.n3()) for
                           (c, s, p, o)
                           in ch], {'source': 'store', 'format': 'tuple', 'state': 'streaming',
                                    'response_to': self.sink.message_id,
                                    'submitted_on': calendar.timegm(datetime.now().timetuple()),
                                    'submitted_by': self.sink.submitted_by}

            lock.acquire()
            try:
                if self.sink.delivery == 'pushing' or (self.sink.delivery == 'mixing' and not self.sink.stream):
                    self.sink.delivery = 'sent'
                    log.info(
                        'The response stream of request {} is completed. Notifying...'.format(self.sink.request_id))
                    yield (), {'state': 'end'}
                elif self.sink.delivery == 'mixing' and self.sink.stream:
                    self.sink.delivery = 'streaming'
            finally:
                lock.release()

    def fragment(self, timestamp):
        def __read_contexts():
            contexts = fragment_contexts(self.sink.fragment_id)
            triple_patterns = {context: eval(context)[1] for context in contexts}
            # Yield triples for each known triple pattern context
            for context in contexts:
                for (s, p, o) in cache.get_context(context):
                    yield triple_patterns[context], s, p, o

        if timestamp is None:
            timestamp = calendar.timegm(dt.now().timetuple())

        from_streaming = not is_fragment_synced(self.sink.fragment_id)

        if from_streaming:
            triples = load_stream_triples(self.sink.fragment_id, timestamp)
            return triples, True
        else:
            return __read_contexts(), from_streaming

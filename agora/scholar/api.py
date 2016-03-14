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

from agora.stoa.server import app
from agora.stoa.store import r
from agora.stoa.server import NotFound
from flask import make_response
from agora.stoa.api import FragmentView, AGENT_ID
from agora.scholar.daemons.fragment import fragment_graph, fragment_lock

__author__ = 'Fernando Serena'


def get_fragment(**kwargs):
    fid = kwargs['id']
    lock = fragment_lock(fid)
    lock.acquire()
    try:
        updated = r.get('{}:fragments:{}:updated'.format(AGENT_ID, fid))
        if updated:
            kwargs['last_updated'] = updated
            kwargs['triples'] = len(fragment_graph(fid))
        pulling = r.get('{}:fragments:{}:pulling'.format(AGENT_ID, fid))
        kwargs['pulling'] = False if pulling is None else eval(pulling)

        return kwargs
    finally:
        lock.release()


FragmentView.decorators.append(get_fragment)


@app.route('/fragments/<fid>/graph')
def get_fragment_graph(fid):
    if not r.sismember('{}:fragments'.format(AGENT_ID), fid):
        raise NotFound('The fragment {} does not exist'.format(fid))

    lock = fragment_lock(fid)
    lock.acquire()
    try:
        response = make_response(fragment_graph(fid).serialize(format='turtle'))
        response.headers['Content-Type'] = 'text/turtle'
    finally:
        lock.release()

    return response

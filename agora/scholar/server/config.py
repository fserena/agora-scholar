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

import logging

import os

__author__ = 'Fernando Serena'


def _params_conf(def_on_demand_th, def_sync_time, def_frag_collectors, def_max_conc_fragments, def_max_conc_deliveries,
                 def_collect_throttling):
    return {'on_demand_threshold': float(os.environ.get('COLLECT_ON_DEMAND_TH', def_on_demand_th)),
            'min_sync_time': int(os.environ.get('FRAGMENT_MIN_SYNC_TIME', def_sync_time)),
            'fragment_collectors': int(os.environ.get('N_FRAGMENT_COLLECTORS', def_frag_collectors)),
            'max_concurrent_fragments': int(os.environ.get('MAX_CONCURRENT_FRAGMENTS', def_max_conc_fragments)),
            'max_concurrent_deliveries': int(os.environ.get('MAX_CONCURRENT_DELIVERIES', def_max_conc_deliveries)),
            'collect_throttling': int(os.environ.get('COLLECT_THROTTLING', def_collect_throttling))}


class Config(object):
    PARAMS = _params_conf(2.0, 10, 8, 8, 4, 10)


class DevelopmentConfig(Config):
    DEBUG = True
    LOG = logging.DEBUG
    STORE = 'persist'


class TestingConfig(Config):
    DEBUG = False
    LOG = logging.DEBUG
    TESTING = True
    STORE = 'memory'


class ProductionConfig(Config):
    DEBUG = False
    STORE = 'persist'

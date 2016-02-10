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

from setuptools import setup, find_packages
import json

__author__ = 'Fernando Serena'

with open("agora/scholar/metadata.json", 'r') as stream:
    metadata = json.load(stream)

setup(
    name=metadata.get('name'),
    version=metadata.get('version'),
    author=metadata.get('author'),
    author_email=metadata.get('email'),
    description=metadata.get('description'),
    license="Apache 2",
    keywords=["linked-data", "ontology", "fragments"],
    url=metadata.get('github'),
    download_url="https://github.com/smartdeveloperhub/agora-scholar/tarball/{}".format(metadata.get('version')),
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['agora', 'agora.scholar'],
    install_requires=['Agora-Stoa'],
    classifiers=[],
    scripts=['scholar'],
    package_dir={'agora.scholar': 'agora/scholar'},
    package_data={'agora.scholar': ['metadata.json']},
)

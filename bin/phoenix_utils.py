#!/usr/bin/env python
############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
############################################################################

import os
import sys
import fnmatch

def find(pattern, classPaths):
    paths = classPaths.split(os.pathsep)

    # for each class path
    for path in paths:
        # remove * if it's at the end of path
        if ((path is not None) and (len(path) > 0) and (path[-1] == '*')) :
            path = path[:-1]
    
        for root, dirs, files in os.walk(path):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    return os.path.join(root, name)
                
    return ""


def setPath():
 global current_dir
 current_dir = os.path.dirname(os.path.abspath(__file__))
 global phoenix_jar_path
 phoenix_jar_path = os.path.join(current_dir, "..", "phoenix-assembly", "target")
 global phoenix_client_jar
 phoenix_client_jar = find("phoenix-*-client.jar", phoenix_jar_path)
 global phoenix_test_jar_path
 phoenix_test_jar_path = os.path.join(current_dir, "..", "phoenix-core", "target")
 global testjar
 testjar = find("phoenix-*-tests.jar", phoenix_test_jar_path)

 if phoenix_client_jar == "":
    phoenix_client_jar = find("phoenix-*-client*", os.path.join(current_dir, ".."))

 if testjar == "":
    testjar = find("phoenix-*-test*", os.path.join(current_dir, ".."))

 return ""

#!/usr/bin/env python
#
#
# Copyright 2013 LinkedIn Corp. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
import sys;
import os;
import os.path;
sys.path.append('integration-test/script');
import run_base;
script_dir = os.getcwd()
module_name = "relay"

if __name__=="__main__":
  print "\n--- Invoking run.py target with arguments ---\n"
  print sys.argv
  run_base.main_base(sys.argv[1:], script_dir, module_name)


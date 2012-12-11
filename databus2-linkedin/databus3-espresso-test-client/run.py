#!/usr/bin/env python
import sys;
import os;
import os.path;
sys.path.append('integration-test/script');
import run_base;
script_dir = os.getcwd()
module_name = "espresso_client"

if __name__=="__main__":
  print "\n--- Invoking run.py target with arguments ---\n"
  print sys.argv
  run_base.main_base(sys.argv[1:], script_dir, module_name)


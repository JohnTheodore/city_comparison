#!/usr/bin/env python
"""
A helper script to run test commands from the .travis.yml file locally
in the dev environment.
"""

from termcolor import cprint
import subprocess
import time
# pylint: disable=E0401
import yaml
from termcolor import cprint

START_TIME = time.time()

START_TIME = time.time()

with open('.travis.yml', 'r') as filehandler:
  try:
    TRAVIS_CONFIG = yaml.safe_load(filehandler)
  except yaml.YAMLError as exc:
    print(exc)

TEST_COMMANDS = TRAVIS_CONFIG.get('script')

EXIT_ERRORS = False
PROCESSES = []
print('')
for test_command in TEST_COMMANDS:
  PROCESSES.append(
    subprocess.Popen(test_command,
                     shell=True,
                     stderr=subprocess.PIPE,
                     stdout=subprocess.PIPE))

for process in PROCESSES:
  exit_code = process.wait()
  if exit_code == 0:
    cprint('PASSED: {}'.format(process.args), 'green')
    print('')
    continue
  EXIT_ERRORS = True
  cprint('FAILED: {}'.format(process.args), 'red')
  print(process.stdout.read().decode('ascii'))
  print(process.stderr.read().decode('ascii'))
  print('')

MINUTES_TO_COMPLETE = round((time.time() - START_TIME) / 60, 1)
cprint('All tests ran in: {} minutes.'.format(MINUTES_TO_COMPLETE), 'green')
print('')
if EXIT_ERRORS:
  exit(1)

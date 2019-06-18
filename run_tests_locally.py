#!/usr/bin/env python
"""
A helper script to run test commands from the .travis.yml file locally
in the dev environment.
"""

import subprocess
# pylint: disable=E0401
import yaml
from merging_code.utils import get_logger, stop_watch_function

LOGGER = get_logger('run_tests_locally')


def get_test_commands_to_run():
  """ Get a list of all commands from script on .travis.yml """
  with open('.travis.yml', 'r') as filehandler:
    try:
      travis_config = yaml.safe_load(filehandler)
    except yaml.YAMLError as exc:
      LOGGER.error(exc)
  return travis_config.get('script')


def run_commands(test_commands):
  """ Run commands with subprocess.Popen, return list of subprocess objects. """
  processes = []
  for test_command in test_commands:
    processes.append(
      subprocess.Popen(test_command,
                       shell=True,
                       stderr=subprocess.PIPE,
                       stdout=subprocess.PIPE))
  return processes


def check_and_report_on_test_commands(ran_commands):
  """ If subprocess commands exit non-zero, so will this script.
  We only print stdout or stderr when subprocess commands exit non-zero. """
  exit_errors = False
  print('')
  for process in ran_commands:
    exit_code = process.wait()
    if exit_code == 0:
      LOGGER.info('\u001b[32mPASSED: {}\n'.format(process.args))
      continue
    exit_errors = True
    LOGGER.error('FAILED: {}\n'.format(process.args))
    print(process.stdout.read().decode('ascii'))
    print(process.stderr.read().decode('ascii'))
  if exit_errors:
    exit(1)


def run_tests_locally():
  """ Run all the travis script test commands locally. """
  travis_test_commands = get_test_commands_to_run()
  commands_ran = run_commands(travis_test_commands)
  check_and_report_on_test_commands(commands_ran)


stop_watch_function(LOGGER, run_tests_locally)

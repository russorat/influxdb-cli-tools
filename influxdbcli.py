#!/usr/local/bin/python3

import argparse, toml, time, codecs
from os.path import expanduser
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException
from urllib3.exceptions import NewConnectionError
from urllib3 import Retry
from datetime import datetime
import pytz


def tasks_retry(client, args):
    if args.all_failed:
        tasks_api = client.tasks_api()
        runs = tasks_api.get_runs(task_id=args.task_id, limit=500)
        run_map = {}
        retried_runs = []
        for run in runs:
            run_scheduled_for = str(run.scheduled_for)
            if not run_map.get(run_scheduled_for):
                run_map[run_scheduled_for] = run
            else:
                if not run.started_at or run.started_at > run_map.get(run_scheduled_for).started_at:
                    run_map[run_scheduled_for] = run
        for run in run_map.values():
            if run.status == "failed": 
                print("Retrying run: {}".format(run.id))
                new_run = tasks_api.retry_run(task_id=args.task_id,run_id=run.id)
                print("New run: {} has a status of: {}".format(new_run.id,new_run.status))
                retried_runs.append(new_run)
                time.sleep(1)
        if len(retried_runs) == 0:
            print("No failed runs detected in the last 500 runs.")
        else:
            _monitor_runs(tasks_api, retried_runs)
    else:
        print("Nothing to do.")

def tasks_runs(client, args):
    utc=pytz.UTC
    tasks_api = client.tasks_api()
    if args.run_id:
        run = tasks_api.get_run(task_id=args.task_id, run_id=args.run_id)
        _print_task_run(run)
        _print_task_log(run)
    else:
        runs = []
        runs = tasks_api.get_runs(task_id=args.task_id, limit=args.limit)
        for run in runs:
            if (args.after and run.scheduled_for >= utc.localize(datetime.strptime(args.after, '%Y-%m-%dT%H:%M:%SZ'))) or (not args.after):
                _print_task_run(run)

def _print_task_run(run):
    print("Task ID: {}, Task Run ID: {}, Status: {}, Started: {}, Scheduled: {}, Finished: {}, Duration: {} seconds".format(run.task_id,run.id,run.status,run.started_at,run.scheduled_for,run.finished_at, (run.finished_at - run.started_at).seconds))

def _print_task_log(run):
    for log in run.log:
        if log.message and log.message.startswith("Started task from script:"):
            print(log.message.split('"',1)[0])
            flux_script = codecs.getdecoder('unicode_escape')(log.message.split('"',1)[1])[0][:-1]
            count = 0
            for line in flux_script.splitlines():
                print("{}:{}".format(count,line))
                count+=1
        else:
            print(log.message)

def _monitor_runs(tasks_api, runs):
    while len(runs) > 0:
        for run in runs:
            try:
                updated_run = tasks_api.get_run(task_id=run.task_id, run_id=run.id)
                print("Run {} has a status: {}".format(updated_run.id,updated_run.status))
                if updated_run.status == "success" or updated_run.status == "failed":
                    runs.remove(run)
            except ApiException as e:
                if e.status == 404:
                    print("Run {} has not started yet.".format(run.id))
                else:
                    print(e)
            time.sleep(2)
          
def parseArguments():
    home = expanduser("~")
    parser = argparse.ArgumentParser(prog='influxdbcli',
                                    description='A set of tools for interacting with the InfluxDB APIs.',
                                    epilog='Find me on GitHub: https://github.com/russorat/influxdb-cli-tools')

    # Global Arguments
    parser.add_argument("-c","--active-config", help="Influx CLI config profile to use", default="default")
    parser.add_argument("--configs-path", help="Path to the influx CLI configurations", default=home + "/.influxdbv2/configs")

    subparsers = parser.add_subparsers()

    tasks_parser = subparsers.add_parser('tasks', help='Commands for working with tasks.')
    tasks_subparser = tasks_parser.add_subparsers()
    
    tasks_retry_subparser = tasks_subparser.add_parser('retry', help='Commands for retrying tasks.')
    tasks_retry_subparser.add_argument("-i", "--task-id",help="The id of the task to operate on.", required=True)
    tasks_retry_subparser.add_argument("--all-failed",help="Retry all task runs that have never had a successful completion.", action="store_true")
    tasks_retry_subparser.set_defaults(func=tasks_retry)

    tasks_runs_subparser = tasks_subparser.add_parser('runs', help='Commands for task runs.')
    tasks_runs_subparser.add_argument("-i", "--task-id",help="The id of the task to operate on.", required=True)
    tasks_runs_subparser.add_argument("-l", "--limit",help="The number of runs to display (<= 500).", default=100, type=int)
    tasks_runs_subparser.add_argument("-a", "--after",help="Only show runs scheduled after this time (2020-10-04T03:00:00Z).")
    tasks_runs_subparser.add_argument("-r", "--run-id",help="The run id of the task to operate on.")
    tasks_runs_subparser.set_defaults(func=tasks_runs)
    
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    args = parseArguments()
    
    with open(args.configs_path) as f:
        parsed_toml = toml.loads(f.read())
        config = parsed_toml[args.active_config]
        run = True
        if not config:
            print("Valid config not found")
            run = False
        if run:
            try:
                retries = Retry(status=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=2, allowed_methods=["GET", "PUT", "DELETE", "OPTIONS", "POST"])
                client = InfluxDBClient(url=config["url"], token=config["token"], org=config["org"], retries=retries, debug=False)
                args.func(client, args)
            except NewConnectionError as e:
                print("Unable to connect to InfluxDB: {}".format(e))
            finally:
                client.close()

        
            
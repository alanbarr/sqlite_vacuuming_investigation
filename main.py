#!/usr/bin/env python3

import sqlite3
import os
import time
import multiprocessing
import argparse

import monitor
import result
import sqlite_scenarios
import config


class ScenarioHandles:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.monitor_pipe = None
        self.monitor_stop_event = None


def write_versioning():
    with open(config.RESULT_DIR + "versions.txt", "w") as version_file:
        print(
            f"SQLite Python Module Version: {sqlite3.version_info}", file=version_file
        )
        print(f"SQLite3 version: {sqlite3.sqlite_version}", file=version_file)
        print(f"num_rows: {config.NUM_ROWS_IN_DB}", file=version_file)


def start_monitor(td, result_file):

    (td.monitor_pipe, pipe_receive) = multiprocessing.Pipe()

    td.monitor_stop_event = multiprocessing.Event()

    monitor_process = multiprocessing.Process(
        name="monitor",
        target=monitor.monitor,
        kwargs={
            "stop_event": td.monitor_stop_event,
            "action_log_receiver": pipe_receive,
            "db_file": config.DB_FILE,
            "temp_dir": config.TMP_DIR,
            "result_file": result_file,
        },
    )
    monitor_process.start()
    return monitor_process


################################################################################

scenarios = {
    0: sqlite_scenarios.scenario_00_large_write_single_commit,
    1: sqlite_scenarios.scenario_01_large_write_multiple_commits,
    2: sqlite_scenarios.scenario_02_large_write_single_commit_checkpoint_tuncate,
    3: sqlite_scenarios.scenario_03_small_write_large_delete_transaction,
    10: sqlite_scenarios.scenario_10_large_transaction_vacuum_populated_db,
    11: sqlite_scenarios.scenario_11_small_transaction_vacuum_populated_db,
    12: sqlite_scenarios.scenario_12_small_transaction_vacuum_populated_checkpoint_db,
    20: sqlite_scenarios.scenario_20_vacuum_previously_populated_db,
    21: sqlite_scenarios.scenario_21_vacuum_and_checkpoint_previously_populated_db,
    30: sqlite_scenarios.scenario_30_delete_and_entire_incremental_vacuum_first_15,
    31: sqlite_scenarios.scenario_31_delete_and_entire_incremental_vacuum_last_15,
    32: sqlite_scenarios.scenario_32_delete_and_entire_incremental_vacuum_first_60,
    33: sqlite_scenarios.scenario_33_delete_and_entire_incremental_vacuum_last_60,
    34: sqlite_scenarios.scenario_34_delete_and_entire_incremental_vacuum_all_100,
    35: sqlite_scenarios.scenario_35_delete_and_entire_incremental_vacuum_last_3_checkpoint,
    40: sqlite_scenarios.scenario_40_delete_and_granular_incremental_vacuum_first_15,
    41: sqlite_scenarios.scenario_41_delete_and_granular_incremental_vacuum_last_15,
    42: sqlite_scenarios.scenario_42_delete_and_granular_incremental_vacuum_first_60,
    43: sqlite_scenarios.scenario_43_delete_and_granular_incremental_vacuum_last_60,
    44: sqlite_scenarios.scenario_44_delete_and_granular_incremental_vacuum_first_15_checkpoint,
    45: sqlite_scenarios.scenario_45_delete_and_granular_incremental_vacuum_last_15_checkpoint,
    50: sqlite_scenarios.scenario_50_delete_first_15,
    51: sqlite_scenarios.scenario_51_delete_last_15,
    60: sqlite_scenarios.scenario_60_delete_and_granular_incremental_vacuum_last_15_595,
    61: sqlite_scenarios.scenario_61_delete_and_granular_incremental_vacuum_last_15_596,
    62: sqlite_scenarios.scenario_62_delete_and_granular_incremental_vacuum_last_15_595,
    63: sqlite_scenarios.scenario_63_delete_and_granular_incremental_vacuum_last_15_596,
    64: sqlite_scenarios.scenario_64_delete_and_granular_incremental_vacuum_last_15_200,
    65: sqlite_scenarios.scenario_65_delete_and_granular_incremental_vacuum_last_15_200,
    66: sqlite_scenarios.scenario_66_delete_and_entire_incremetal_last_15_no_autocheckpoint,
}


def run_scenario(scenario):

    print(f"Running scenario {scenario}")
    td = ScenarioHandles()

    os.makedirs(config.RESULT_DIR, exist_ok=True)
    result_file = f"{config.RESULT_DIR}/results_scenario_{scenario}"
    monitor_process = start_monitor(td, result_file)

    sqlite_scenarios.setup_database(td, config.DB_FILE)
    scenarios[scenario](td)

    sqlite_scenarios.cleanup_database(td)

    print(f"Waiting for scenario {scenario} to finish ...")

    time.sleep(3)
    td.monitor_stop_event.set()
    monitor_process.join()

    print(f"Finished scenario {scenario}")


################################################################################

os.makedirs(config.TMP_DIR, exist_ok=True)
os.environ["SQLITE_TMPDIR"] = config.TMP_DIR

os.makedirs(config.RESULT_DIR, exist_ok=True)
write_versioning()

parser = argparse.ArgumentParser(prog="SQLiteWAL")
parser.add_argument("--scenario", type=int, choices=scenarios)
args = parser.parse_args()


if args.scenario is not None:
    run_scenario(args.scenario)
else:
    for scen_id in scenarios.keys():
        run_scenario(scen_id)

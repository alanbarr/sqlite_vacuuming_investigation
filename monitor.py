#!/usr/bin/env python3

import os
import time
import result

import pickle


def get_size_or_zero(filename):
    try:
        return os.stat(filename).st_size
    except FileNotFoundError:
        return 0


def get_file_sizes(db, shm, wal, tmp_dir):

    try:
        tmp_dir_size = sum(f.stat().st_size for f in os.scandir(tmp_dir))
    except FileNotFoundError:
        # Lazy - give it another shot
        tmp_dir_size = sum(f.stat().st_size for f in os.scandir(tmp_dir))

    return result.FileSize(
        get_size_or_zero(db), get_size_or_zero(shm), get_size_or_zero(wal), tmp_dir_size
    )


def monitor(stop_event, action_log_receiver, db_file, temp_dir, result_file):
    db_shm_file = db_file + "-shm"
    db_wal_file = db_file + "-wal"

    result_list = result.ResultList()

    while stop_event.is_set() is False:
        if action_log_receiver.poll():
            result_list.add(action_log_receiver.recv())
        result_list.add(get_file_sizes(db_file, db_shm_file, db_wal_file, temp_dir))
        time.sleep(0.2)

    result_list.write_csv(result_file + ".csv")

    with open(result_file + ".pickled", "wb") as pickle_file:
        pickle.dump(result_list, pickle_file)

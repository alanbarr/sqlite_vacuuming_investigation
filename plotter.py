#!/usr/bin/env python3

import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pickle
import result
import glob

import config

cm = 1 / 2.54


def to_mb(byte_count):
    return byte_count / (10 ** 6)


def plot_file_data(file_name, show_fig):

    with (open(file_name, "rb")) as pickled_file_object:
        results = pickle.load(pickled_file_object)

    x = list()
    y_db = list()
    y_wal = list()
    y_tmp = list()
    y_shm = list()

    initial_timestamp = results.l[0].timestamp

    for r in results.l:
        if not isinstance(r, result.FileSize):
            continue
        delta = (r.timestamp - initial_timestamp).total_seconds()
        x.append(delta)

        y_db.append(to_mb(r.db))
        y_wal.append(to_mb(r.wal))
        y_tmp.append(to_mb(r.tmp_dir))
        y_shm.append(to_mb(r.shm))

    big_graph = True if x[-1] > 180 else False

    ratio = 1.414
    height = 15 * cm / ratio
    width = x[-1] / 6 * cm if big_graph else 18 * cm
    plt.figure(figsize=(width, height))

    plt.plot(x, y_db, label="db", color="blue", linewidth=2)
    plt.plot(x, y_wal, label="wal", color="orange", linewidth=2)
    plt.plot(x, y_tmp, label="tmp", color="green", linewidth=2)

    plt.xlabel("Seconds (s)")
    plt.ylabel("Megabyte (MB)")

    x_ticks_increment = 10

    axes = plt.gca()
    axes.set_xticks(range(0, int(x[-1]) + 1, x_ticks_increment))
    axes.set_xticks(range(0, int(x[-1]) + 1), minor=True)
    axes.set_yticks([0, 4, 20, 40, 60, 80, 85, 90, 95, 100, 105, 110])
    axes.grid(axis="y", alpha=0.4)

    plt.title(results.title)

    for r in results.l:

        if not isinstance(r, result.Action):
            continue

        delta = (r.timestamp - initial_timestamp).total_seconds()
        plt.axvline(x=delta, color="red", alpha=0.3, linestyle="dashed")

        plt.text(
            x=delta,
            y=50,
            s=r.msg,
            rotation=90,
            horizontalalignment="right",
            verticalalignment="center",
            alpha=0.8,
            color="red",
            fontsize="small",
        )

    plt.legend(loc=2)

    plt.savefig(file_name + ".png", dpi=250)

    if show_fig:
        plt.show()

    plt.close()


def plot_single_file(file_name, show_plot):
    plot_file_data(file_name, show_plot)


def plot_all_files_in_dir(directory):
    print("Plotting all files...")
    pickled_files = glob.glob(directory + "/*.pickled")
    for f in pickled_files:
        print(f"Plotting {f}...")
        plot_file_data(f, False)
    print("... done")


parser = argparse.ArgumentParser(prog="SQLiteWALPlotter")
parser.add_argument(
    "result",
    default=False,
    type=str,
    help="Result folder to plot all results, or a single pickled result file",
)
parser.add_argument(
    "--show_plot",
    action="store_true",
    help="Show the plot. Only valid when when single file provided as result.",
)
args = parser.parse_args()

if os.path.isdir(args.result):
    plot_all_files_in_dir(args.result)

elif os.path.isfile(args.result):
    plot_single_file(args.result, args.show_plot)
else:
    print(f"Not valid results: {args.results}")

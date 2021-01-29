# About

This repository contains scripts to attempt to profile SQLite database file
sizes when Write Ahead Logging (WAL) mode is enabled.

This isn't the most elegant code and went through some iterations, but should
hopefully be "good enough" for its intended purpose.
For example, it was originally intended to just produce ".csv" files of the
results and interpret them in LibreOffice/Excel. However, midway through the
investigation it was decided to experiment with matplotlib instead. It is
likely evident that this was cobbled together as an afterthought.

This repository contains two executable scripts:

- `main.py`: Starts a process to monitor file sizes then begins running SQLite
test scenarios. The results are saved both as `.csv` files and pickled data
(`.pickled`).
- `plotter.py` Reads in the `.pickled` files and produces graphs using
matplotlib.

The results of running these scripts are available in the `results` directory.
A blog post discussing the results can be found [here](
https://theunterminatedstring.com/sqlite-vacuuming).

# Run a Scenario

The following is an example of the commands required to run and plot a single
scenario. Without an argument, `main.py` will start running all the test
scenarios.

```
./main.py --scenario 0
./plotter.py --show_plot results/${new_timestamped_folder}/results_scenario_0.pickled
```

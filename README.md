"benchmark.py" benchmarks four databases: SQLite, Influx, Tempo, Xively.
For each database, the script performs and times the following operations:
* inserting points
* selecting the first point
* selecting last point
* selecting a range of points

and then exports results for each database as a CSV file.

"benchmark_plots.R" creates plots of the data from the CSV file, so the user can visualize the results.

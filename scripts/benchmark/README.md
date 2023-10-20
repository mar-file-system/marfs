# Benchmarking Suite Overview 

[[_TOC_]]

## Overview

A collection of scripts intended to benchmark MarFS across erasure scheme (N, E, and PSZ) and ranks per node, then visualize the output in a series of heatmaps and bar graphs. 

**MarFS** is a campaign storage system originally developed in 2015 for Los Alamos National Laboratory as operated by Los Alamos National Security, LLC for the United States Department of Energy. See [the project GitHub repository](https://github.com/mar-file-system) for more information.

## Directory Structure

The repository is laid out as follows:

* **bin**: executables and helpers to perform benchmarking and visualization.
	* `rank_benchmark.py`: benchmarks across rank
	* `stripe_benchmark.py`: benchmarks across N, E, PSZ
	* **graphing**: graphing-specific executables and helpers.
		* `execute.sh`: takes arguments and automates graph generation
		* utility graphing scripts
		* subdirectory-specific `README` detailing usage
	* misc. utility scripts
	* subdirectory-specific `README` detailing script usage and functionality
* **data**: default location to store benchmarking run data.
	* **graphs**: default location to store graph script output.
* **etc**: auxiliary files or alternative script implementations.
	* `marfs-config-ex.xml`: sample MarFS config used as template for erasure scheme benchmarking.
	* `pftool-ex.cfg`: sample pftool config.
	* **altscripts**
		* alternative implementation of stripe benchmarking (Python script with Bash helper).
* **wiki**: additional information on performance concerns and dependencies.
	* misc. markdown files

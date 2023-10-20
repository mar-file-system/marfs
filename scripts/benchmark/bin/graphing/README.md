# MarFS Benchmark Graphing Tools

## Description
A set of scripts that serve to generate heat graphs and bar graphs that map performance of the MarFS file system. They take in input of csv files with the following information: stripewidth, parity blocks, data blocks, partsize, bandwidth, amd read or write. The graphs are made with tools from the Seaborn package. Both scripts are run from the bash script execute.sh with the name of the csv file as your given variable. The x, y, and z variables on the graphs can be configured in the bash script if you wish to plot other information.

## Tools.py
Provide the tools used by both Heatmap.py and Linegraph.py. This file has the move function that puts the graph pngs into the correct folder in the directory. Graphs are stored in a folder named after the time of day they were made. This directory is inside another directory named after the date the graphs were made. The other role of Tools.py is to take in the shorthand abbreviations of the csv file and convert them to their full name (i.e. bw -> bandwidth and PSZ -> partsize)

## Heatmap.py
Generates a heatmap for each partsize in the CSV. The script takes in one variable: a csv file with values to graph. For each unique partsize, the heatmapGraph function is called to generate the heatmap. The script uses Seaborn to make the graphs.
The following is the syntax to run the file:
### python3 Heatmap.py input.csv 
with input.csv being the name of the CSV with the graph data

## Bargraph.py
Generates a bar graph for any given combination of data blocks and parity blocks. The input is passed in as the script is run formatted as data blocks then parity blocks (i.e. 10 2). The graph function starts by isolating the CSV by the given parity and data block values. From there, the remaining information is passed into Seaborn to generate a bar graph. The graph uses partsize as the x axis and bandwidth as the y axis. There are two columns for each partsize: the blue one is write values, and the red is read values.
The following is the syntax to run the file:
### python3 Bargraph.py input.csv N E
with input.csv being the csv with graph data, N being number of data blocks, and E being number of parity blocks

## RankBargraph.ph
Only to be run on CSV files containing a rank row. Can be run by uncommenting execute line in execute.sh or by running individually. This is meant to compare rank and bandwidth to determine fastest rank. It runs by isolating the rank and bandwidth values and graping them against each other at a given data block/parity block value.
The following is the syntax to run the file:
### python3 RankBargraph file.csv N E 
where file.csv is the input file, N is number of data blocks, and E is number of parity blocks

## Usage
Running this program is all done by calling the bash script (execute.sh) and providing the following variables: reference CSV, number of data blocks for the bar graph, and number of parity blocks for the bar graph. The CSV is what the program will use for all the information needed to make the graphs. The number of data blocks and parity blocks will tell the Bargraph.py file which combination will be used to make the graph. 

## Sample Run
### bash execute.sh data-2023-07-07_17:23:37.039749.csv 10 2
This run will generate read and write heat maps using the data from data-2023-07-07_17:23:37.039749.csv. The same data will be used to generate a bar graph with 10 data blocks and 2 parity blocks. 

## CSV Header Format
CSV files must contain the following rows in any order:
### pb,db,PSZ,bw

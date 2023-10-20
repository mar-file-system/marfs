import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys
from Tools import *

def barGraph(infile, a, b):    #function that plots a bar graph
    print("Generating bar graph...")   
    data=pd.read_csv(infile)   #reads in csv file given by user
    sns.set_theme(context='paper')    #sets theme of graph to paper context and dark style
    sns.set(rc = {'figure.figsize':(90,70)})    #resizes graph for higher resolution
    sns.set(rc = {'figure.facecolor':'dce6f2'}, font_scale=8) #changes background color and resizes font
    dataSort= data.query(f"pb == {b} & db == {a}")  #isolates the csv to only show the rows containing the given parity and data blocks values.

    plt.clf()       #clears graph canvas
    ax = sns.barplot(x='PSZ', y='bw', data=dataSort, hue='mode')     #plots out the reads without the confidence interval
    plt.xticks(rotation=45)
    ax.grid(axis='y',b=True, which='major', color='black', linewidth=1) #grids only on y axis, black color and 1 width

    ax.set(xlabel="Partsize (bytes)", ylabel="Throughput (GB/sec)")  #names the x axis and y axis
    plt.legend(loc='lower left')    #anchors the graph legend to only show on the bottom left corner of the graph
    title=("Partsize vs Throughput at N = " + a + " and  E = " + b)    #titles of the graph
    ax.set_title(title, fontdict={'size':160})   #sets the title and resizes title font 
    plt.savefig("Bar_Graph_DB_" + a + "_PB_" + b + ".png")   #saves the graph with its given name
    
    moveFiles()     #moves pngs into the graphs directory
    print("Done!")
    return

def main():     #main function
    infile = sys.argv[1]    #user input of the name of the csv file that will be graphed
    a = sys.argv[2]     #user input on what data blocks they wish to graph
    b = sys.argv[3]     #user input on parity blocks to graph

    barGraph(infile, a, b)

if __name__ == "__main__":
    main()

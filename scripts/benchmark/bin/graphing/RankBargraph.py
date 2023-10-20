import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys
from Tools import *

def barGraph(infile):       #function to graph bar graph 
    print("Generating rank bar graph...")
    data=pd.read_csv(infile)    #read in csv
    sns.set(rc = {'figure.figsize':(57,50)})    #resizes the graph for higher resolution
    sns.set(rc = {'figure.facecolor':'dce6f2'}, font_scale=4)    #changes color around graph and resizes font
    plt.clf()   #clears the graph area
    plt.xticks(rotation=45) 
    ax=sns.barplot(x='rank', y='bw', data=data, hue='mode', orient='v') #plots the graph on empty canvas
    ax.grid(axis='y',b=True, which='major', color='black', linewidth=1) #makes a y axis grad with black color and width of 1
    ax.set(xlabel="Rank", ylabel="Throughput (GB/sec)")  #labels x and y axes
    
    PSZ = str(data.iat[2,3])    #gets graphs PSZ value
    pb = str(data.iat[2,2])     #^^ for parity blocks
    db = str(data.iat[2,1])     #^ for data blocks

    plt.legend(loc='lower left')    #anchors legend to bottom left corner
    title = ("Minimum ranks per node vs Throughput at N = "+db+" E = "+pb+" and PSZ = "+PSZ) #title for graph
    ax.set_title(title, fontdict={'size':100})   #sets title and resizes title font
    plt.savefig("Bar_Graph_Rank.png")       #saves graph 

    moveFiles()     #moves graph into appropriate file
    print("Done!")
    return

def ifRank(infile):
    data = pd.read_csv(infile)
    if "rank" in data.columns:
        return True
    else:
        return False

def main():     #main function
    infile = sys.argv[1]    #input csv
    if (ifRank(infile) == True):
        barGraph(infile)    #calls graph making function
    else:
        print("No ranks in this file! Aborting rank graph generation!")
        return

if __name__ =="__main__":
    main()

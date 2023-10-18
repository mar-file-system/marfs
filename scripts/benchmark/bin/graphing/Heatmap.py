import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys
import os
import shutil
from Tools import *

def heatmapGraph(infile,mode,i,csvMin,csvMax):  #function that graphs heatmap. Takes in csv file, variables to be graphed, r or w, and psz
    print("Generating heat graph...")
    inpt=pd.read_csv(infile)    #makes dataframe of the given csv file
    inCSV = inpt[inpt['PSZ'] == i]  #makes a temporary dataframe for given psz value
    inCSV = inCSV[inCSV['bw'] >= 0]     #skips all rows with negative bandwidths
    sns.set_theme(context='paper',style='dark')     #sets the graph theme to have paper context and dark style
    sns.set(rc = {'figure.figsize':(55,30)})
    sns.set(rc = {'figure.facecolor':'dce6f2'}, font_scale=6)
    csv=inCSV.pivot_table(index='pb',columns='db',values='bw')   #makes a pivot table with the given variables 
    plt.clf()   #clears the graph to prepare for a new print
    plt.xticks(rotation=45)

    ax=sns.heatmap(csv,cmap="plasma",vmin=csvMin,vmax=csvMax)   #graphs out a heatmap with the sorted dataframe and a plasma cmap
    
    x="Parity Blocks"    #sets the full name for the pb variable
    y="Data Blocks"    # ^^ for db
    z="Throughput"    # ^ for bw
    ax.invert_yaxis()   #inverts the y axis so that it counts upwards
    ax.set(xlabel=y,ylabel=x)   #names the x and y labels
    t = ""      #temp string
    if (mode == 'r'):   #if this is a read graph
        t = "READ"      #set temp string to read
    elif(mode == 'w'):  #if it's write
        t = "WRITE"     #set string to write
   
    title = ("Throughput in GB/sec ("+t+"S) PSZ = "+str(i))  #titles graph
    ax.set_title(title, fontdict={'size':100})   #sets the title and resizes title font
    plt.savefig("Heat_Map_"+t+"S_PSV_"+str(i)+".png")    #saves graph as a png
    moveFiles()     #calls function to move graphs into a png folder
    print("Done!")
    return

def readOrWrite(incsv,i,csvMin,csvMax):  #function to determine if the mode is read or write
    r = 'r'     #temp r char
    w = 'w'     #temp w char
    rOW = pd.read_csv(incsv)    #makes a datarame with the given file
    reads = pd.DataFrame()      #makes a reads dataframe
    writes = pd.DataFrame()     #makes a writes dataframe
    rCSV = ("reads.csv")        #makes a new reads csv
    wCSV = ("writes.csv")       #makes a new writes csv
    
    df_read = rOW.loc[rOW['mode']=='r']     #reads in all rows with a read value into read dataframe
    df_write = rOW.loc[rOW['mode']=='w']    #reads in all rows with write value into write dataframe
    
    df_read.to_csv(r'reads.csv')    #puts read dataframe into read csv
    df_write.to_csv(r'writes.csv')  #^^ for writes
    
    heatmapGraph(rCSV,r,i,csvMin,csvMax) #calls heatmap graph function with the given reads csv
    heatmapGraph(wCSV,w,i,csvMin,csvMax) #does the same with writes csv
    
    os.remove("reads.csv")      #removes the temp reads csv
    os.remove("writes.csv")     #removes the tmep writes csv
    return

def sortPSZ(infile):   #function to sort the csv by partsize
    data = pd.read_csv(infile)  #reads in csv file
    uniques = data['PSZ'].unique()  #new variable that holds all unique psz values

    csvMin = data['bw'].min()   #new variable equal to the minimum value found in the bandwidth column
    csvMax = data['bw'].max()   # ^^ maximum value in bandwidth

    for i in uniques:   #iterate through every value in uniques
        readOrWrite(infile, i, csvMin, csvMax)  #calls readOrWrite function for each unique PSZ value

def main():     #Main function
    infile=sys.argv[1]  #takes in csv file
    sortPSZ(infile)    #calls the function that sorts by partsize value
    
    copyCSV(infile)
    

if __name__ == "__main__":
    main()

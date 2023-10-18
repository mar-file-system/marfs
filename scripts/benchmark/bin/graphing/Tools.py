import os
import shutil
from datetime import date
import time

def moveFiles():    #function to moves graph pngs into correct directory
    source = os.getcwd()    #get source directory where file is currently located
    dt = date.today()       #todays date to name archive file
    now = time.localtime()    #get current time to name archive file
    now = time.strftime("%H:%M", now)   #gets the time in hours and minutes
    t = (str(dt) + '/' + str(now)) #combines date and time into one file path
    
    dest = (os.getcwd() + '/../../data/graphs/Graphs-' + t)    #get file path where pngs will go 
    if not os.path.exists(dest):     #if that directory does not exist
        os.makedirs(dest)       #make the new directory
    files = os.listdir(source)      #list that holds all file names in source directory
    for i in files:     #iterate through the files list
        if ((i.startswith("Heat_"))or(i.startswith("Bar_"))):  #if its heat or line graph
            shutil.move(i,dest)     # moves pngs into the destination directory
    return t

def copyCSV(infile):
    dt = date.today()       #todays date to name archive file
    now = time.localtime()    #get current time to name archive file
    now = time.strftime("%H:%M", now)   #gets the time in hours and minutes
    t = (str(dt) + '/' + str(now)) #combines date and time into one file path
    
    src = infile
    destination = (os.getcwd()+'/../../data/graphs/Graphs-'+t)
    
    shutil.copy(src, destination)

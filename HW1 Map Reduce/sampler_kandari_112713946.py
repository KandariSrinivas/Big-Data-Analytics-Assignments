##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: Srinivas Kandari
## Student ID: 112713946

##Data Science Imports: 
import numpy as np
import csv
from datetime import datetime

import mmh3
import random


##IO, Process Imports: 
import sys
from pprint import pprint


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 2):
    # Implements the standard non-streaming sampling method
    # Step 1: read file to pull out unique user_ids from file
    # Step 2: subset to random  1% of user_ids
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn

    mean, standard_deviation = 0.0, 0.0

    ##<<COMPLETE>>
    columns = ["record_id", 'date', 'user_id', 'amount']
    dt = np.dtype([('record_id', np.int64), ('date', np.unicode_, 24),('user_id', np.int64), ('amount', np.float64) ])
#    ## Add For loop - Stream
    userList = {}
    for line in filename:
        line = line.split(',')
        trans = np.array([(line[0], line[1], line[2], line[3] )], dtype=dt )
        userList[trans[0][columns[sample_col]]] = 1

    unique = userList.keys()
    
    sample = []
    for id in unique:
        if(random.randint(0, 100) <= percent*100):
            sample.append(id)
    count=0
    filename.seek(0)
    
    for line in filename:
        line = line.split(',')
        trans = np.array([(line[0], line[1], line[2], line[3])], dtype = dt)
        
        if trans[0][columns[sample_col]] in sample:
#            sampl_trans.append(trans)
            count = count+1
            distFromMean = trans['amount'] - mean
            mean = mean + ((distFromMean) / count)
            standard_deviation = standard_deviation + (distFromMean) * (trans['amount'] -mean)
# #    print(count, ":  COUNT")
#     mean = mean / count
# #    print(standard_deviation, "SD before ")
    standard_deviation = ((standard_deviation/(count-1)))**0.5
    

    return mean[0], standard_deviation[0]


    

##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler
    
############################# HASH FUNCTION ##############################
def hash(key):   #Bucket Size: 100
    return key % 100


def streamSampler(stream, percent = .01, sample_col = 2):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    mean, standard_deviation = 0.0, 0.0
    columns = ["record_id", 'date', 'user_id', 'amount']
    dt = np.dtype([('record_id', np.int64), ('date', np.unicode_, 24),('user_id', np.int64), ('amount', np.float64)])
    count=0
    ##<<COMPLETE>>
    for line in stream:
        line = line.split(',')
        trans = np.array([(line[0], line[1], line[2], line[3] )], dtype=dt )
        if(hash(trans[columns[sample_col]]) < percent * 100):
            
            count = count+1
            distFromMean = trans['amount'] - mean
            mean = mean + (distFromMean/count)
            standard_deviation = standard_deviation + (distFromMean)*(trans['amount'] - mean)
      
        
    # mean = mean / count
    standard_deviation = ((standard_deviation / (count)))**0.5
#    print("count: ", count, "percent: ", percent, "mean: ", mean, "SD: ", standard_deviation)
#            
#    print(count, " : COUNT")
    ##<<COMPLETE>>

    return mean[0], standard_deviation[0]


##########################################################################
##########################################################################
# Task 1.C Timing

files=['transactions_small.csv', 'transactions_medium.csv']
percents=[.02, .005]

#     fstream = open('transactions_large.csv', 'r')
#     answer = streamSampler(fstream)

# if False: 
if __name__ == "__main__":
    
    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            fstream = open(f, "r")
            d1 = datetime.now()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            print("     Time taken : ", (datetime.now() - d1).total_seconds() * 1000)
            fstream.close()
            fstream = open(f, "r")
            d1 = datetime.now()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            print("     Time taken : ", (datetime.now() - d1).total_seconds() * 1000)
    

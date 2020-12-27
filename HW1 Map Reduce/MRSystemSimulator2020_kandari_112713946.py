##########################################################################
## MRSystemSimulator2020.py  v 0.1
##
## Implements a basic version of MapReduce intended to run
## on multiple threads of a single system. This implementation
## is simply intended as an instructional tool for students
## to better understand what a MapReduce system is doing
## in the backend in order to better understand how to
## program effective mappers and reducers. 
##
## MyMapReduce is meant to be inheritted by programs
## using it. See the example "WordCountMR" class for 
## an exaample of how a map reduce programmer would
## use the MyMapReduce system by simply defining
## a map and a reduce method. 
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: Kandari Srinivas
## Student ID: 112713946
##Data Science Imports: 
import numpy as np
from scipy import sparse
import mmh3
from random import random
import math


##IO, Process Imports: 
import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint


##########################################################################
##########################################################################
# MapReduceSystem: 

class MapReduce:
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3, use_combiner = False): 
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks
        self.use_combiner = use_combiner #whether or not to use a combiner within map task
        
    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): 
        print("Need to override map")

    
    @abstractmethod
    def reduce(self, k, vs): 
        print("Need to overrirde reduce")
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r, combiner=False): 
        #runs the mappers on each record within the data_chunk and assigns each k,v to a reduce task
        mapped_kvs = [] #stored keys and values resulting from a map 
        for (k, v) in data_chunk:
            #run mappers:
            # if k == 10:
            #     print("4th Chunk : ", k)
            chunk_kvs = self.map(k, v) #the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) 
			
	#assign each kv pair to a reducer task
        if combiner:
            #do the reduce from here before passing to reduceTask

            #<<COMPLETE>>
            # print("In combiner")
            # if k==10:
            #     print("Last map Task before Combbner: ", mapped_kvs)
            pass
            #1. Setup value lists for reducers (Local Partitions)
            #<<COMPLETE>>  
            combined = dict()
            for key, v in mapped_kvs:
                try:
                    combined[key].append(v)
                except KeyError:
                    combined[key] = [v]

            #2. call reduce, appending result to get passed to reduceTasks (Local Reduce Outputs)
            #<<COMPLETE>>
            results = []
            # if k == 10:
            #     print("Local Partitons for 4th chunk : ", combined)
            for (key, vs) in combined.items():
                results.append(self.reduce(key, vs))
            # if k == 10:
            #     print("Local Reduce Results for 4th chunk : ", results)
            
               
            for (k, v) in results:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))

        else:
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k): 
        #given a key returns the reduce task to send it

        ##<<COMPLETE>>
        node_number = 0
        seed=0
        try:
            if type(k) == str:
                for i in k:
                    seed = seed + ord(i)
            else:
                seed = k
            node_number = seed % self.num_reduce_tasks  ## TODO:  should i use num of nodes?
        except:
            ## failed to calculate seed
            node_number = random.randint(0, self.num_reduce_tasks)

        return node_number


    def reduceTask(self, kvs, namenode_fromR): 
        #sort all values for each key (can use a list of dictionary)
        # TODO: is kvs the complete data or only the part
        vsPerK = dict()
        for (k, v) in kvs:
            try:
                vsPerK[k].append(v)
            except KeyError:
                vsPerK[k] = [v]


        #call reducers on each key with a list of values
        #and append the result for each key to namenoe_fromR
        for k, vs in vsPerK.items():
            # if(k == 'w'):
            #     print("Array of w: ", vs)
            if vs:
                fromR = self.reduce(k, vs)
                if fromR:#skip if reducer returns nothing (no data to pass along)
                    namenode_fromR.append(fromR)

		
    def runSystem(self): 
        #runs the full map-reduce system processes on mrObject

	#the following two lists are shared by all processes
        #in order to simulate the communication
        namenode_m2r = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                          #in the form [(k, v), ...]
        
	#Divide up the data into chunks according to num_map_tasks
        #Launch a new process for each map task, passing the chunk of data to it. 
        #Hint: The following starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        runningProcesses = []
        ## <<COMPLETE>>

        chunkSize = math.ceil(len(self.data) / self.num_map_tasks)
        for i in range(0, self.num_map_tasks):
            p = Process(target = self.mapTask, args=(self.data[chunkSize*i : chunkSize*(i+1)], namenode_m2r, self.use_combiner))
            p.start()
            runningProcesses.append(p)


	#join map task running processes back
        for p in runningProcesses:
            p.join()
		        #print output from map tasks 
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

	#"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)] 
        ## <<COMPLETE>>
        for (reducer_no, (k, v)) in namenode_m2r:
            to_reduce_task[reducer_no].append((k,v))
        
        #launch the reduce tasks as a new process for each. 
        runningProcesses = []
        for kvs in to_reduce_task:
            runningProcesses.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR)))
            runningProcesses[-1].start()

        #join the reduce tasks back
        for p in runningProcesses:
            p.join()
        #print output from reducer tasks 
        print("namenode_fromR after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        #return all key-value pairs:
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:
            
class WordCountBasicMR(MapReduce): #[DONE]
    #mapper and reducer for a more basic word count 
	# -- uses a mapper that does not do any counting itself
    def map(self, k, v):
        kvs = []
        counts = dict()
        for w in v.split():
            kvs.append((w.lower(), 1))
        return kvs

    def reduce(self, k, vs): 
        return (k, np.sum(vs))  

#an example of another map reducer
class SetDifferenceMR(MapReduce): 
    #contains the map and reduce function for set difference
    #Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v):
        toReturn = []
        for i in v:
            toReturn.append((i, k))
        return toReturn

    def reduce(self, k, vs):
        if len(vs) == 1 and vs[0] == 'R':
            return k
        else:
            return None

class MeanCharsMR(MapReduce): #[TODO]
    def map(self, k, v):
        pairs = []
        v = v.lower()
        counts = [0] * 26
        for char in v:
            if ord(char) >= ord('a') and ord(char) <= ord('z'):
                index = ord(char) - ord('a')
                counts[index] = counts[index] + 1
        for i in range(0,len(counts)):
            pairs.append((chr(ord('a') + i), (counts[i], 0, 1)))
            # if(chr(ord('a') + i) == 'c' and counts[i] ==7):
            #     print("sentence : ", v)

        #<<COMPLETE>>
        # print("pairs", pairs)
        return pairs
        
    
    def reduce(self, k, vs):
        value = None
        # if(k== 'a'):
        #     print("values of a", vs)
        #     if(len(vs) == 1):
        #         print("ALert! combiner on last maptask : ", vs)
        #<<COMPLETE>>
        elements = []
        samples = []

        for v in vs:
            if v[2] == 1:
                elements.append(v)
            elif v[2] > 1:
                samples.append(v)

        count, mean, SD = 0, 0, 0
        if len(elements) == 1:
            (mean, SD, count) = elements[0]
        # elif len(elements) > 1:
        else:
            for element in elements:
                count+=1
                distFromMean = element[0] - mean
                mean = mean + ((distFromMean)/count)
                SD = SD + (element[0] - mean) * (distFromMean)
            try:
                SD = (SD/(count))**0.5
            except:
                pass
        if (not len(samples)):
            # print("No Combiner used")
            return (k, (mean, SD, count))

        samples.append((mean, SD, count))
        (mean1, SD1, count1) = samples[0]
        for (mean2, SD2, count2) in samples[1:]:
            mean12 = (count1/(count1 + count2)) * mean1 + (count2/(count1 + count2)) * mean2

            SD12 = ( ((count1*(SD1**2 + (mean12-mean1)**2)) + (count2*(SD2**2 + (mean12 - mean2)**2))) / (count1+count2) )**0.5
            count12 = count1 + count2
            (mean1, SD1, count1) = (mean12, SD12, count12)
            
        value = (mean1, SD1, count1)
        return (k, value)



        # if len(vs) and type(vs[0]) == int:
        #     sum, squared_sum = 0, 0
        #     for i in range(0, len(vs)):
        #         sum = sum + vs[i]
        #         squared_sum = squared_sum + vs[i]**2
        #     mean = round(sum / len(vs), 2)
        #     SD = (round(squared_sum/len(vs), 2) - mean**2)
        #     value = (mean, SD, len(vs))
        #     return (k, value)
        # elif len(vs) and type(vs[0]) == tuple:
        #     sum, SD, count = 0,0,0
        #     for i in range(0, len(vs)):
        #         # if vs[i][2] > 1:

        #         count = count + vs[i][2]
        #         sum = sum + vs[i][0] * vs[i][2]
        #         SD = SD + vs[i][1] * vs[i][2]
        #     mean = round(sum/ count,2)
        #     SD = round(SD/count,2)
        #     value = (mean, SD, count)
        #     return (k, value)
        # else: ## should be the case because even if the letter didn't appear in any we send array of zero with total no of docs being length of array
        #     return (k, (0,0,0))

			
			
##########################################################################
##########################################################################

from scipy import sparse
def createSparseMatrix(X, label):
	sparseX = sparse.coo_matrix(X)
	list = []
	for i,j,v in zip(sparseX.row, sparseX.col, sparseX.data):
		list.append(((label, i, j), v))
	return list

if __name__ == "__main__": #[Uncomment peices to test]
    
    ###################
    ##run WordCount:
    
    # print("\n\n*****************\n Word Count\n*****************\n")
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful")]
    # print("\nWord Count Basic WITHOUT Combiner:")
    # mrObjectNoCombiner = WordCountBasicMR(data, 3, 3)
    # mrObjectNoCombiner.runSystem()
    # print("\nWord Count Basic WITH Combiner:")
    # mrObjectWCombiner = WordCountBasicMR(data, 3, 3, use_combiner=True)
    # mrObjectWCombiner.runSystem()
    

    ###################
    ##MeanChars:
    print("\n\n*****************\n Word Count\n*****************\n")
    data.extend([(8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
                 (9, "The car raced past the finish line just in time."),
	         (10, "Car engines purred and the tires burned.")])
    print("\nMean Chars WITHOUT Combiner:")
    mrObjectNoCombiner = MeanCharsMR(data, 4, 3)
    mrObjectNoCombiner.runSystem()
    print("\nMean Chars WITH Combiner:")
    mrObjectWCombiner = MeanCharsMR(data, 4, 3, use_combiner=True)
    mrObjectWCombiner.runSystem()
      


	
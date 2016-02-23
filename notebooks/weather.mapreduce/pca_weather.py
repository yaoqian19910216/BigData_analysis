#!/usr/bin/python
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
import re,pickle,base64,zlib
from mrjob.job import MRJob
from sys import stderr
import pandas as pd
import numpy as np
import sklearn as sk
import gzip
import pickle

f = gzip.open('ML_station_hash.pkl.gz', 'rb' )
pickleFile = pickle.Unpickler(f)
hashtable = pickleFile.load()
f.close()

class PCA_weather(MRJob): 

    #This loads the lookup file into a field on the object
    def load_data(self):
        global hashtable
        f = gzip.open('ML_station_hash.pkl.gz', 'rb' )
        pickleFile = pickle.Unpickler(f)
        hashtable = pickleFile.load()
        f.close()
    
    def mapper_pre(self, _, line):
        global hashtable
        elements = line.split(',')
        if elements[1] == 'TMAX' or elements[1] == 'TMIN':
            region = hashtable.get(elements[0])
            if region != None:
                yield (elements[0], elements[2], region), (elements[1], elements[3:])
                
    def reducer_pre(self, station_pair, weathers):
        weather_max = []
        weather_min = []
        valid_count = 0
        data_count = 0
        for weather in weathers:
            if weather[0] == 'TMAX': weather_max = weather[1]
            if weather[0] == 'TMIN': weather_min = weather[1]
            data_count += 1
        if data_count == 2 and len(weather_max) == 365 and len(weather_min) == 365:
            for i in range(365):
                if weather_max[i] != '' and weather_min[i] != '':
                    valid_count += 1
            if valid_count >= 180:
                weather_data = weather_max + weather_min
                yield station_pair[2], weather_data
            
    def reducer_pca(self, region, matrixs):
        M = pd.DataFrame(matrixs)
        M[M == ''] = float('NaN')
        M = M.astype(float)
        M = M.transpose()
        (columns,rows)= np.shape(M)
        Mean = np.mean(M, axis=1).values
        C=np.zeros([columns,columns])  
        N=np.zeros([columns,columns])

        for i in range(rows):
            row = M.iloc[:,i] - Mean
            outer = np.outer(row,row)
            valid = np.isnan(outer) == False
            C[valid] = C[valid]+ outer[valid]
            N[valid] = N[valid]+ 1
            
        valid_outer = np.multiply(1 - np.isnan(N),N>0)
        cov = np.divide(C,N)
        cov = np.multiply(cov, valid_outer)
        U, D, V = np.linalg.svd(cov)
        cum_sum = np.cumsum(D[:])/np.sum(D)
        for i in range(len(cum_sum)):
            if cum_sum[i] >= 0.99:
                ind = i 
                break
        yield region, [ind, cum_sum.tolist()]
    
    def steps(self):
        return [self.mr(mapper = self.mapper_pre, reducer=self.reducer_pre),
                self.mr(reducer = self.reducer_pca)]
        
if __name__ == '__main__':
    PCA_weather.run()
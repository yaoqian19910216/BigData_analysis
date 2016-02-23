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

class split_weather(MRJob): 
    
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
            
    def reducer_split(self, region, matrixs):
        for matrix in matrixs:
            yield (region, matrix)
           
    def steps(self):
        return [self.mr(mapper = self.mapper_pre, reducer=self.reducer_pre),
                self.mr(reducer = self.reducer_split)]
        
if __name__ == '__main__':
    split_weather.run()
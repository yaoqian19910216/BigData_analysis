from __future__ import division
import sys
from mrjob.job import MRJob
import re
from sys import stderr

class Countstationmeasurement(MRJob):
    
    #mapper extract out the TMAX or TMIN year measurement of each station
    #elements[0] corresponds to the station name
    #elements[1] corresponds to the year
    def mapper1(self, _, line):
        elements = line.split(',')
        if elements[1] == 'TMIN' or elements[1] == 'TMAX':
            yield [elements[0], elements[2]], elements[3:]
    
    #reducer1 decides whether the measurement in this station is a good measurement
    #Input Data_vectors may only contain TMAX or TMIN year measurement, or both year measurement
    #If this station's year measurement have both TMAX  and TMIN measurement more than 150 days,
    #It's counted as a valid measurement
    def reducer1(self, station_year_pair, Data_vectors):
        Data_vectors = list(Data_vectors)
        if len(Data_vectors) < 2:
            yield station_year_pair[0], 0
        else:
            TMINTMAXday_count = 0 
            for i in range(len(Data_vectors[0])):
                if Data_vectors[0][i] != '' and Data_vectors[1][i] != '': 
                    TMINTMAXday_count += 1
            if TMINTMAXday_count < 150 :
                yield station_year_pair[0], 0
            else:
                yield station_year_pair[0], 1
    
    #reducer 1 will count the number of valid measurement in each station
    def reducer2(self, station, vector_valid):
        stationmeasurement = sum(vector_valid)
        yield station, stationmeasurement
    
    def steps(self):
        return [self.mr(mapper=self.mapper1, reducer=self.reducer1),
                self.mr(reducer=self.reducer2)]

if __name__ == '__main__':
    Countstationmeasurement.run()
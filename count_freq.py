# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 09:11:50 2022

@author: Ncu
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class myclass(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.mapper1,reducer=self.reducer1),
                MRStep(reducer=self.reducer2)
            ]
    
    def mapper1(self,_,line):
        for word in line.split():
            yield word,1
    
    def reducer1(self,key,values):
        yield None , (sum(values),key)
            
        
    def reducer2(self, _, values):
        yield max(values)
        

if __name__ == '__main__':
    myclass.run()
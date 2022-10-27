from mrjob.job import MRJob

class myclass(MRJob):
    def mapper(self,_,line):
        for word in line.split():
            yield "count",1
            
        
    def reducer(self, key, values):
        yield key , sum(values)
        

if __name__ == '__main__':
    myclass.run()
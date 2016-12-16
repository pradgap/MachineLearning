import numpy as np
from numpy import array
from math import sqrt
from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
def main(sc):
    rawRdd=sc.textFile('network_PCA_15Features2.csv')
#   print rdd.count()
#   rddTrain, rddTest= rdd.randomSplit([.8,.2]) 
#   print 'Done with random split' 
    rdd=rawRdd.map(lambda line: parseData(line))
    parsedRdd=rdd.map(lambda line: line[0:-1])
    model=KMeans.train(parsedRdd,7,maxIterations=10,runs=10, initializationMode='random')
    print model.centers
    error=parsedrdd.map(lambda line:WGSE(line,model)).reduce(lambda a,b:a+b)
    print 'The error for WGSE is ::'
    print error
    clusterCount=rdd.map(lambda line:cluster(line,model)).reduceByKey(lambda a,b: np.add(a,b))
    print clusterCount.collect()
        
def WGSE(line,model):
    predict=model.predict(line)
    center=model.centers[predict]
    return sqrt(sum([x**2 for x in (line-center)]))

def parseData(line):
    split=line.split(',')
    return array([float(x) for x in split])
def cluster(line,model):
    list=[0]*23
    list[int(float(line[-1]))]=1
    predict=model.predict(line[0:-1])
#   clusterList[predict][line[-1]]=clusterList[predict][line[-1]]+1
    return (predict,list)
    
if __name__=="__main__":
    conf=SparkConf().setAppName('sparkApp')
    conf=conf.setMaster("local[*]")
    sc=SparkContext(conf=conf)
    main(sc)

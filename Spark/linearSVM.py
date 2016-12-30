

from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint


def main(sc):
	rawRDD=sc.textFile('/home/pradhangaurav_55/SparkDir/network_PCA_15Features2.csv')
	rddTrain,rddTest=rawRDD.randomSplit([0.7,0.3])
	parsedTrain=rddTrain.map(lambda line: parse(line))
	parsedTest=rddTest.map(lambda line: parse(line))
	model=SVMWithSGD.train(parsedTrain,iterations=100)
	
	labelsAndPreds=parsedTest.map(lambda p: (p.label,model.predict(p.features)))
	trainErr= labelsAndPreds.filter(lambda (v,p): v!=p).count()/float(parsedTest.count())
	print "Training error is ::::::::::   %f  " %trainErr

def parse(line):
	values=[float(x) for x in line.split(',')]
	if (int(values[15])==11):
		return LabeledPoint(1,values[0:15])
	else:
		return LabeledPoint(0, values[0:15])
	




if __name__=='__main__':
	conf=SparkConf().setAppName('linearSVM')
	conf=conf.setMaster('local[*]')
	sc=SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	main(sc)

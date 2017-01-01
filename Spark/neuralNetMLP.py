from pyspark import SparkContext, SparkConf
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.ml.classification import MultilayerPerceptronClassifier


def main(spark):

	rdd=spark.sparkContext.textFile('/home/pradhangaurav_55/SparkDir/network_PCA_15Features2.csv')
	rddTrain,rddTest=rdd.randomSplit([0.7,0.3])
	df= rddTrain.map(lambda line:parse(line)).toDF(['label','features'])
        mlp = MultilayerPerceptronClassifier(maxIter=100, layers=[15, 18, 23], blockSize=1, seed=123)
        model = mlp.fit(df)
        # model.layers
        # model.weights.size
        testDF= rddTest.map(lambda line:parse(line)).toDF(['label','features'])
        predict=model.transform(testDF)
	predictionAndLabel=predict.select('prediction','label')
	evaluator=MulticlassClassificationEvaluator(metricName="accuracy")
	accuracy= evaluator.evaluate(predictionAndLabel)
	print 'Accuracy for Multilabel Perceptron is  :: '+ str(accuracy)

def parse(line):
		values=[float(x) for x in line.split(',')]
		return (values[15], Vectors.dense(values[0:15]))

def parseTest(line):
		values=[float(x) for x in line.split(',')]
		return (Vectors.dense(values[0:15]),)

if __name__=='__main__':
	spark = SparkSession \
                .builder \
                .appName("Python Spark SQL basic example") \
                .master('local[*]') \
                .getOrCreate()
        main(spark)

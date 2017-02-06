To run the application, at current directory, execute the following command:
./setup.bash

train: trainmodel.py


input format:
	"id";"polarity";"tweet"
	e.g. sts_gold_tweet.csv
	polarity:
		0: negtive
		4: positive


output:
	rf_model.model

steps:

	1. replace all the spaces(\s) with " "		(line: 27)
	2. replace all the urls with " "			(line: 28)
	3. tokenize									(line: 30)
	4. filter stop words						(line: 32) 	(default stop words by spark, might need to change)
	5. convert to vector: Word2Vec				(line: 34)
	6. index label								(line: 36)
	7. split training test to tain and test		(line: 40)
	8. train 									(line: 48)
	9. test										(line: 50)
	10.save model 								(line: 63)


model: 

	random forest 								(line: 42)

	from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
	https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.RandomForestClassifier

	RandomForestClassifier(self, featuresCol="features", labelCol="label", predictionCol="prediction", probabilityCol="probability", rawPredictionCol="rawPrediction", maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", numTrees=20, featureSubsetStrategy="auto", seed=None)

	in which:
		featuresCol: 		vectorized words 	(step: 5)
		labelCol: 			indexed polarity  	(step: 6)	(0.0: negtive, 1.0: positive)
		other parameters: 	default, might need to change





classification: sentimentAnalysis.py

input: 	short.json

output: 	sentiments.json/part-r-00000xxxxxxx.json

steps:

	1. load model 								(line: 26)
	2. replace all the spaces(\s) with " "		(line: 32)
	3. replace all the urls with " "			(line: 34)
	4. tokenize									(line: 36)
	5. filter stop words						(line: 38) 	(default stop words by spark, might need to change)
	6. convert to vector: Word2Vec				(line: 40)
	7. test with model 							(line: 42)
	8. format output 							(line: 47~55)
	9. count predicate percentage 				(line: 49) 	(because 0: negtive, 1:positive, directly take the average)







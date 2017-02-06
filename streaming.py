from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.mllib.clustering import StreamingKMeans
#from prediction_func import prediction_func_RF
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import *
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import json
import Queue,time
import thread
import io

def prediction_func_RF(rdd, rfModel, q):
	# read from rdd
    spark = SparkSession.builder.appName("RF_Predict").getOrCreate()
    schema = StructType([StructField("clusters", IntegerType(), True),\
						StructField("keyword", StringType(), True),\
						StructField("center_lat", DoubleType(), True),\
						StructField("center_lon", DoubleType(), True),\
						StructField("tweets_num", IntegerType(), True),\
						StructField("text", StringType(), True)])
    data = spark.createDataFrame(rdd, schema)
    df = data.withColumn('text', encode(data.text, 'US-ASCII'))
    df = df.withColumn('text', regexp_replace('text', "\?+(\s|$)", ""))
    df = df.withColumn('texts_normed', regexp_replace('text', "(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?(\s|$)", ""))\
		   .withColumn('texts_normed', regexp_replace('texts_normed', "@\w*(\s|$)", ""))\
		   .withColumn('texts_normed', regexp_replace('texts_normed', "[\!\.\:;,](\s|$)", " "))\
		   .withColumn('texts_normed', regexp_replace('texts_normed', "\w+('s|'re|'ve|'d|'m)(\s|$)", ""))\
		   .withColumn('texts_normed', regexp_replace('texts_normed', "[\s]+", " "))
    df = df.withColumn('texts_normed', ltrim(df.texts_normed))
    df = df.withColumn('texts_normed', rtrim(df.texts_normed))

    tokenized = Tokenizer(inputCol="texts_normed", outputCol="words").transform(df)
    filtered = 	StopWordsRemover(inputCol="words", outputCol="filtered").transform(tokenized).drop('words')

	# prediction
    pred = rfModel.transform(filtered)

	# format the structed data and send data
    structed = pred.withColumnRenamed("predictedLabel", "text_sentiment")\
					.withColumn("tweets", struct("text","text_sentiment"))\
					.groupBy("clusters","center_lat","center_lon","keyword")\
					.agg({"text_sentiment":"avg","tweets": "collect_list"})\
					.withColumnRenamed("avg(text_sentiment)", "keyword_sentiment")\
					.withColumnRenamed("collect_list(tweets)", "tweets")\
					.withColumn("trends", struct("center_lat","center_lon","keyword","keyword_sentiment","tweets"))\
					.groupBy("clusters").agg(collect_list("trends")).withColumnRenamed("collect_list(trends)", "trends")\
					.select("trends")
    res = structed.toJSON().collect()
    q.put(res)
    #structed.coalesce(1).write.json("json-result", mode="overwrite")

def getLocation(tweet):
    location = tuple()
    try:
        if tweet['coordinates'] == None:
            location = tweet['place']['bounding_box']['coordinates']
            location = reduce(lambda x, nxt: [x[0] + nxt[0], x[1] + nxt[1]], location[0])
            location = tuple(map(lambda t: t / 4.0, location))
        else:
            location = tuple(tweet['coordinates']['coordinates'])
    except TypeError:
        #print ('error get_coord')
        location=(0,0)
    return location

def getTags(myjson):
    tags = myjson['entities']['hashtags']
    tagList = list()
    for tag in tags:
        if 'text' in tag:
            tagList.append(tag['text'].lower()) # I add a lower() here
    return tagList

def get_json(myjson):
  try:
    json_object = json.loads(myjson)
    if not 'text' in json_object:
      return False
  except ValueError, e:
    return False
  return json_object

def splitList(l):
    ret = []
    for tag in l[0]:
        ret.append((tag,l[1]))
    return ret

def includeCluster(data):
    cluster = data[1]
    data[0]['cluster'] = cluster
    return data[0]

def findTrends(l):
    tops = 0
    cluster_size = len(l)
    if cluster_size<50:
        tops = 7
    elif cluster_size>=50 and cluster_size<200:
        tops = 15
    elif cluster_size>=200 and cluster_size<500:
        tops = 25
    else:
        tops = 40
    sorted_list = sorted(l, key=lambda i: i[2], reverse=True)
    return sorted_list[0:tops]

def formatOutput(myjson):
    ret = {}
    ret['text'] = myjson['text'] 
    ret['id'] = myjson['id']
    ret['timestamp'] = myjson['timestamp_ms']
    userInfo = {}
    userInfo['id'] = myjson['user']['id']
    userInfo['followers_count'] = myjson['user']['followers_count']
    userInfo['friends_count'] = myjson['user']['friends_count']
    ret['user'] = userInfo
    ret['location'] = getLocation(myjson)
    ret['tags'] = getTags(myjson)
    return ret

def formatOutput_2(myjson):
    ret = {}
    ret['text'] = myjson['text']
    # ret['user_id'] = myjson['user']['id']
    # ret['text_sentiment'] = 0
    return ret

def formatOutput_3(l):
    # ( cluster, [( (cluster, tag),[json],len([json]) )] )
    ret = {}
    ret['clusters'] = l[0]
    trends = []
    for item in l[1]:
        x = 0
        y = 0
        for i in range(item[2]):
            x = x + item[1][i]['location'][0]
            y = y + item[1][i]['location'][1]
        attribute = {}
        attribute['keyword'] = item[0][1]
        attribute['center_lat'] = x/item[2]
        attribute['center_lon'] = y/item[2]
        # attribute['keyword_sentiment'] = 0
        attribute['tweets_num'] = len(item[1])
        attribute['tweets'] = list(map(formatOutput_2,item[1]))
        trends.append(attribute)
    ret['trends'] = trends
    # op = json.dumps(ret)
    return ret

def writeQueue(q):
    #c = getConnection()

    while True:
        if q.empty():
            time.sleep(12)
        else:
            with io.open('sample.txt', 'w', encoding='utf-8') as f:
                while not q.empty():
                    obj=q.get()
                    #print(q.qsize())
                    for t in obj:
                        f.write(t)
                        f.write(unicode('\n', 'UTF-8'))

q = Queue.Queue()
thread.start_new_thread( writeQueue, (q, ) )
sc   = SparkContext()
ssc = StreamingContext(sc, 35 )
sqlContext = SQLContext(sc)
socket_stream = ssc.socketTextStream("localhost", 5555)
lines = socket_stream.window( 70 )
rfModel = PipelineModel.load("rf_16000.model")

jsonLines = lines.map(lambda l: get_json(l))\
            .filter(lambda p: p != False)\
            .filter(lambda l: len(l['entities']['hashtags']))\
            .map(formatOutput)

# clustering
model = StreamingKMeans(k=25, decayFactor=0.6).setRandomCenters(2, 1.0, 9527)
training_data = jsonLines.map(lambda l: l['location'])
test_data = jsonLines.map(lambda l: (l, l['location']))
model.trainOn(training_data)
pred_data = model.predictOnValues(test_data).map(includeCluster)

# trend calculation
cluster_trend_data = pred_data.map(lambda l: (l,l['tags'])).flatMapValues(lambda l: l)\
                    .map(lambda l: ((l[0]['cluster'],l[1]),l[0])).groupByKey().mapValues(list)\
                    .map(lambda l: (l[0][0],(l[0],l[1],len(l[1])))).groupByKey().mapValues(findTrends)\
                    .map(formatOutput_3)
#cluster_trend_data.pprint(1000)
# make prediction of sentiment and send result
prediction_data = cluster_trend_data.map(lambda l: (l['clusters'],l['trends'])).flatMapValues(lambda l: l)\
                .map(lambda l: ((l[0], l[1]['keyword'], l[1]['center_lat'],l[1]['center_lon'],l[1]['tweets_num']),l[1]['tweets']))\
                .flatMapValues(lambda l: l)\
                .map(lambda l: (l[0][0],l[0][1],l[0][2],l[0][3],l[0][4],l[1]['text']))
# prediction_data.pprint(1000)

predicted_data = prediction_data.foreachRDD(lambda data: prediction_func_RF(data, rfModel, q))
# predicted_data.foreachRDD(lambda rdd: q.put(rdd.collect()))



ssc.start()# Start the computation
ssc.awaitTermination()






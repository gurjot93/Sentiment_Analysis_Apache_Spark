
# coding: utf-8

# In[1]:


"""
Configuring sparkSession
"""
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# In[2]:


"""
Reading data from Tweets.csv
"""
import pandas as pd
import re
# Created dataFrame from Tweets.csv
fields = ['airline_sentiment','text']
dataset = pd.read_csv("Tweets.csv", usecols=fields)
dataset = dataset.replace({'[0-9]','&','$','@','#',':','-','!','\?','\.','_','\(','\)'},'',regex=True)

print(dataset)


# In[3]:


"""
Creating model
"""
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import col,udf
from pyspark.sql.types import IntegerType
from sklearn.feature_extraction.text import CountVectorizer
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SQLContext  

# Created Spark DataFrame on the Training dataset.
spark_df = spark.createDataFrame(dataset,['airline_sentiment','text'])

indexer = StringIndexer(inputCol="airline_sentiment", outputCol="label").fit(spark_df)
indexed_df = indexer.transform(spark_df)

# Initiated Tokenizer which splits each character in the string(text).
tokenizer = Tokenizer(inputCol="text",outputCol="word")

# Initiated Hashing on the data recieved from word
hashingTF = HashingTF(inputCol="word", outputCol="outFeatures", numFeatures=20)

# Initiated IDF on the data recieved from outFeatures
idf = IDF(inputCol="outFeatures", outputCol="features")

# Initiated Logistic Regression recieved from features
lr = LogisticRegression(maxIter=10,regParam=0.001)

# Providing input to Pipeline
pipeline=Pipeline(stages=[tokenizer,hashingTF,idf,lr])

# Model created
model = pipeline.fit(indexed_df)
print(model)
# Saving the model locally
model.save("assignment3_1.model")


# In[4]:


"""
Testing and analysing the data.
"""
from elasticsearch import Elasticsearch
from elasticsearch import helpers, Elasticsearch
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import json
from tweepy import Stream

es = Elasticsearch(["https://admin:FNQWOUKVZVORTXOU@portal-ssl10-37.bmix-dal-yp-cc38e81f-55f5-4558-96fd-aa84b720bc0a.894976941.composedb.com:58282/"])
consumerKey = "jaCIQyRWTPUmtvVFDeNyt8bPR"
consumerSecret = "Qg5NxNmMmQg5wguKypugehUUKtAqjbcuEoVil9J44phAI95Xii"
accessToken = "1044941494401077248-T1KtDajKhnBEWtmzS48KqssQKbDTrq"
accessTokenSecret = "jiJGValNS0fIU3xP1re4AVMdVmanyzblzHDc5TTv9kbjI"

def readES(): # Use this to get data from ElasticSearch
        listItem=[]
        res = es.search(index="part3", body={},size=2000, from_=0)
        for hit in res['hits']['hits']:
            listItem.append(hit['_source']['message'])
        return pd.DataFrame(listItem)

dFrame =  readES()
dFrame = dFrame.replace({'[0-9]','&','$','@','#',':','-','!','\?','\.','_','\(','\)'},'',regex=True)
finalDF = spark.createDataFrame((dFrame),['text'])

# Prediction of model
prediction1 = model.transform(finalDF)
selected = prediction1.show(2000)


# In[ ]:


"""
Configuring ElasticSearch session
"""
from elasticsearch import Elasticsearch
from elasticsearch import helpers, Elasticsearch
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import json
from tweepy import Stream
es = Elasticsearch(["https://admin:FNQWOUKVZVORTXOU@portal-ssl10-37.bmix-dal-yp-cc38e81f-55f5-4558-96fd-aa84b720bc0a.894976941.composedb.com:58282/"])

# import twitter keys and tokens
consumerKey = "jaCIQyRWTPUmtvVFDeNyt8bPR"
consumerSecret = "Qg5NxNmMmQg5wguKypugehUUKtAqjbcuEoVil9J44phAI95Xii"
accessToken = "1044941494401077248-T1KtDajKhnBEWtmzS48KqssQKbDTrq"
accessTokenSecret = "jiJGValNS0fIU3xP1re4AVMdVmanyzblzHDc5TTv9kbjI"

# create instance of elasticsearch
count = 0
listItem = []

def writeES(dict_data): # Use this to post data to ElasticSearch
        es.index(index="part3",
                 doc_type="test-type",
                 body={"message": dict_data["text"]})


class TweetStreamListener(StreamListener):
              
    #on success        
    def on_data(self, data):
        global count
        
        # decode json
        dict_data = json.loads(data)
        
        # add text and sentiment info to elasticsearch
        writeES(dict_data)
        count = count + 1
        if(count<2000):
            return True
        else:
            return False
     #on fail   
    def on_error(self, status):
        print (status)

   




# create instance of the tweepy tweet stream listener
listener = TweetStreamListener()

# set twitter keys/tokens
auth = OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessTokenSecret)   

# create instance of the tweepy stream
stream = Stream(auth, listener)

# search twitter for "hollywood" keyword
stream.filter(track=['hollywood'])
    


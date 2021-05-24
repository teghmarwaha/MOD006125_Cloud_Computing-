#!/usr/bin/env python
# coding: utf-8

# In[146]:


import math
import sys
import re, string
import findspark
findspark.init('/opt/spark')


# In[147]:



from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession 
from pyspark.sql.types import  StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as f
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id, col, ceil
from pyspark.sql.window import Window

spark = SparkSession                      .builder                      .master("local")                      .appName("sample E")                     .getOrCreate()


sc=spark.sparkContext


# In[148]:


text_file = sc.textFile('/home/tegh/Downloads/sample-e.txt')


# In[149]:


def lower_clean_text(x):
  punc=';:?!"()[]{}-_."",'
  lowercased_text = x.lower()
  for ch in punc:
    lowercased_text = lowercased_text.replace(ch, ' ')
  return lowercased_text


# In[150]:


text_flat = text_file.map(lower_clean_text)

low_text_flat = text_flat.flatMap(lambda x: x.split(' '))


rems_text = low_text_flat.filter(lambda x:len(x)>0) # remove spaces

text_count = rems_text.map(lambda x:(x,1))

wordText = text_count.reduceByKey(lambda x,y : x+y)


output = wordText.collect()


# In[151]:



# Create a schema for the dataframe
schema = StructType([
    StructField('Word', StringType(), True),
    StructField('Count', IntegerType(), True),
    ])

# Convert list to RDD
rD = spark.sparkContext.parallelize(output)

# Create data frame
dF = spark.createDataFrame(rD,schema)

dF_if = dF.orderBy(f.col("Count").desc())


# In[152]:


#Filtering and removing words with numbers and symbols

dF_f1 = dF_if.filter(~col("Word").contains(">")).filter(~col("Word").contains("@")).filter(~col("Word").contains("+")).filter(~col("Word").contains("'s")).filter(~col("Word").contains("'t")).filter(~col("Word").contains("'ll")).filter(~col("Word").contains("'")).filter(~col("Word").contains("/")).filter(~col("Word").contains("`")).filter(~col("Word").contains("’")).filter(~col("Word").contains("“")).filter(~col("Word").contains("—")).filter(~col("Word").contains("$")).filter(~col("Word").contains("%")).filter(~col("Word").contains("^")).filter(~col("Word").contains("&")).filter(~col("Word").contains("=")).filter(~col("Word").contains("£")).filter(~col("Word").contains("*")).filter(~col("Word").contains("~")).filter(~col("Word").contains("1")).filter(~col("Word").contains("2")).filter(~col("Word").contains("3")).filter(~col("Word").contains("4")).filter(~col("Word").contains("5")).filter(~col("Word").contains("6")).filter(~col("Word").contains("7")).filter(~col("Word").contains("8")).filter(~col("Word").contains("9")).filter(~col("Word").contains("0"))

dF_count = dF_f1.withColumn('Rank',row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)


# In[153]:


wordSum = dF_f1.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y)  #total sum

uniqueWords = dF_f1.distinct().count() #distinct sum


# In[154]:


low = (((uniqueWords/100) *5))
lowC = math.ceil(low)
lowCT= (lowC - 1)


# In[155]:


midV1 = ((uniqueWords/100) *47.5)
midV1C = math.floor(midV1)


# In[156]:


midV2 = ((uniqueWords/100) *52.5)
midV2C = math.ceil(midV2)


# In[157]:


high = (((uniqueWords/100) *95)-1)
highC = math.floor(high)
print(highC)


# In[158]:


high = (((uniqueWords/100) *95)-1)
highC = math.floor(high)



# In[159]:


a = "Values for Sample E"
print(a)

b =("The total number of words =")
c = wordSum.collect()[0][1]

print(b,c)

d = ("The total number of distinct =")
e = uniqueWords

print(d,e)

f = ("The popular treshold words =")
g = lowC

print(f,g)

h = ("The common treshold 1 words =")
i = midV1C

print(h,i)

j="The common treshold 2 words ="
k= midV2C

print(j ,k)

l= ("____________________________________________________________________________________________________________")
print(l)

m =("Table for the top values")
print(m)
n = dF_count.filter(col("Rank").between(0,lowCT)).show(100000)




o = ("Table for the mid values")
print(o)
p = dF_count.filter(col("Rank").between(midV1C,midV2C)).show(100000)



q =("Table for the rare values")
print(q)
r = dF_count.filter(col("Rank").between(highC,uniqueWords)).show(100000)




# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





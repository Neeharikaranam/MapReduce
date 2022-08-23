#!/usr/bin/env python
# coding: utf-8

# In[27]:


from pyspark import SparkContext
from operator import add
#sc = SparkContext("local", "app")

path = "Test_files/Test.txt"
rdd = sc.wholeTextFiles(path)

output = rdd.flatMap(lambda file,contents:[(file, word) for word in contents.lower().split()])          .map(lambda file, word: (word,[file]))          .reduceByKey(lambda a,b: a+b)

final = output.collect()

with open('Spark/Output_files/Spark_output_SW.txt','w') as f:
    for i in final:
        newstr=str(i)+"\n"
        f.write(newstr)
    f.close()

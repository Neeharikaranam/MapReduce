#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pyspark import *

input=sc.textFile("../Test_files/Test.txt")
#Flattening the input
wc=input.flatMap(lambda line:line.lower().split(" "))

#Removing the punctuation marks
def remove_punctuation(param):
    marks="?!@#\"$%^*()_-;:[]{},.——\\/"

    for i in marks:
           param=param.replace(i,"")
    return param

rem=wc.map(remove_punctuation)

length_words = rem.map(lambda words: (len(words), words)).distinct().map(lambda word_list: (word_list[0], [word_list[1]])).reduceByKey(lambda x, y: x + y)
length_list = length_words.collect()


with open('../Spark/Output_files/Spark_output_WL.txt','w') as f:
    for i in length_list:
        newstr=str(i)+"\n"
        f.write(newstr)
    f.close()


# In[ ]:





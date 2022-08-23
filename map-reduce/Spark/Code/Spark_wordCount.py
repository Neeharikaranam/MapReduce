#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark import *

input=sc.textFile("Test_files/Test.txt")
#Flattening the input
wc=input.flatMap(lambda line:line.lower().split(" "))

#Removing the punctuation marks
def remove_punctuation(param):
    marks="?!@#\"$%^*()_-;:[]{},.——\\/"

    for i in marks:
           param=param.replace(i,"")
    return param

rem=wc.map(remove_punctuation)
counter=rem.map(lambda word: (word,1))
words=counter.reduceByKey(lambda a,b: a + b)
final=words.collect()
for (i,j) in final:
        print(i,j)


#Saving the output

with open('Spark/Output_files/Spark_output_WC.txt','w') as f:
    for i in final:
        newstr=str(i)+"\n"
        f.write(newstr)
    f.close()




# In[ ]:





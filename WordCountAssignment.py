
#=================================Part 1 : One word Frequency ==================================

import pyspark
#create spark context
sc=pyspark.SparkContext()
#load the text file
inputFl=sc.textFile("gs://1rashmibucket1/bible+shakes.nopunc");
#1)split using " "(it is set by default in split())
#2)Create a new pair RDD by mapping each word to word,1 pair
#3)reduceByKey - performs key reduction using same word
#4)Print sorted output by key
finaloutput=inputFl.flatMap(lambda inputLine : inputLine.split()).map(lambda inputword : (inputword, 1)).reduceByKey(lambda t,w : t+w).sortByKey(True)
#5)Collect the output
print finaloutput.collect()

#==================================Part 2 : Bigram Frequency =====================================

import pyspark
#create spark context
sc=pyspark.SparkContext()
#load the text file
# Use glom() which provides us the facility to use a partition like an array
#We will perform a join operation on all the words using " " string and then split them using the stop delimiter - this will give us sentences 
#in given RDD partition
biggerList = sc.textFile('gs://1rashmibucket1/bible+shakes.nopunc',4).glom()
partionedList=biggerList.map(lambda part: " ".join(part)).flatMap(lambda whole: whole.split("."))
#perfom split operations and then concatenate two adjacent words till the length of the list
twoWordPair = partionedList.map(lambda sentenc:sentenc.split()).flatMap(lambda adjacent: [((adjacent[k],adjacent[k+1]),1) for k in range(0,len(adjacent)-1)])
#once pairs are found perform reduceByKey operation where bigram serves as the key
finalOutput = twoWordPair.reduceByKey(lambda c,d:c+d).map(lambda z:(z[1],z[0])).sortByKey(False)
print finalOutput.collect()

#================================part3 : One word frequency in another datset=======================
from operator import add 
import pyspark
sc=pyspark.SparkContext()
textFl=sc.textFile("gs://1rashmibucket1/bible+shakes.nopunc");
#Get one word frequency in text file
output=textFl.flatMap(lambda line : line.split()).map(lambda word : (word, 1)).reduceByKey(lambda p,b : p+b)

textToBeSerached="but peter took him up saying stand up i myself also am a man and as he talked with him he went in and \
found many that were come together and he said unto them ye know how that it is an unlawful thing for a\
man that is a jew to keep company or come unto one of another nation but god hath shewed me that i should \
not call any man common or unclean therefore came i unto you without gainsaying as soon as i was sent for \
i ask therefore for what intent ye have sent for me and cornelius said four days ago i was fasting until this \
hour and at the ninth hour i prayed in my house and behold a man stood before me in bright clothing".split()

#perform one word frequency in smaller text 
smallListRDD=sc.parallelize(textToBeSerached,2).map(lambda x :(x,1)).reduceByKey(add)
#So right outer join operation between two pair RDDs and take the output. 
joined=output.rightOuterJoin(smallListRDD).map(lambda (x,y) : (x,y[0])).takeOrdered(100, key=lambda x : x[0])
print joined

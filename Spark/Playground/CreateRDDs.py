from pyspark import SparkConf, SparkContext
  
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

#Parrallelized
# create RDDs from Parallelizing data, or from a CSV or existing table

#Pass lines as strings
data = ["Nice day, huh?","How is it going?","Nice Nice and you?"]

#map(func) run split on the strings, get array of strings. 
#each of the original entries is on its respective line
#distData = sc.parallelize(data).map(lambda x:x.lower().split())

#flatMap(func) run split on the strings, get individual words on individual lines
distData = sc.parallelize(data).flatMap(lambda x:x.lower().split())

#MAP-REDUCE
#Note: will take whichever map is most recent/uncommented

#Map - Map data into data pairs
distData = distData.map(lambda x:(x,1))

#Reduce - Combine key value pairs
#Basically, the lambda fires on like keys, and then the function
#executes on the data read...

#instead of adding (lambda x,y: x+y), append Dupe
distData = distData.reduceByKey(lambda x,y: x+" Dupe!" if isinstance(x,str) else "Dupe!")


distData = distData
results = distData.collect()

for result in results:
    print(result)

# prints 1, 2, 3...
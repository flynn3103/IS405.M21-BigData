from pyspark import SparkContext

sc = SparkContext.getOrCreate()
data = range(10)
dist_data = sc.parallelize(data)
print(dist_data.reduce(lambda a, b: a+b))
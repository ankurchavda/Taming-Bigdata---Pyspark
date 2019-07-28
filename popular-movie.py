from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parselines(line):
    data = line.split(",")
    customer = int(data[0])
    rate = float(data[2])
    return (customer,rate)
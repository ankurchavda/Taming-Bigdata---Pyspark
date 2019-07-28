from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parselines(line):
    data = line.split(",")
    customer = int(data[0])
    rate = float(data[2])
    return (customer,rate)

customer_rdd = sc.textFile("customer-orders.csv")
parsed_customer_rdd = customer_rdd.map(parselines)

total_spendings = parsed_customer_rdd.reduceByKey(lambda x,y : x + y)
sorted_spendings = total_spendings.map(lambda (x,y) : (y,x)).sortByKey()
results = sorted_spendings.collect()

for result in results:
    print(str(result[1]) + "\t{:.2f}F".format(result[0])) 
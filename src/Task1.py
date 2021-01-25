from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

#Build and initiate the spark session for leveraging spark
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("csv_processing") \
        .getOrCreate()

#Building the spark context
sc=spark.sparkContext

#Providing the location for the file to be processed
data_file = r"C:\Users\shubh\Documents\Truata_Tasks\groceries.csv"

#Since we have to use the Spark RDD API we are using the sc.textfile as it stores it in a rdd. using the spark.read.text() would not be appropriate in this case as it would store it in a data frame
rdd_read = sc.textFile(data_file)


#Defining a function to split the data based on ,
def funsplit(lines):
      lines = lines.split(',')
      return lines

#Using the flatmap function as we do not want a nested list and we want a flat continuous list
flat_rdd=rdd_read.flatMap(funsplit)

#Calling an action to this so the transformations are applied. As spark follows a lazy transformations approach
flat_rdd.collect()

#Since we have to get the list of individual elements
cnt_dist=flat_rdd.distinct().collect()

#Task 1

#Printing in the required format


f=open('/Users/shubh/Documents/Truata_Output/Task_1/out_1_1.txt','w')
for i in cnt_dist:
    f.write("Product  ")
    f.write(str(i))
    f.write("\n")

f.close()

#Printing the total count

#Task 2

f=open('/Users/shubh/Documents/Truata_Output/Task_1/out_1_2.txt','w')
f.write("The total count is ")
f.write("\n")
f.write(str(flat_rdd.count()))
f.close()

#Task 3

#Since we have flattened out the rdd we can assign a number to each item and then later we can compute the aggregate and for that 
#aggregate we need to have a key value pair to perform aggragation,so converting it to a key value pair
dict_rdd=flat_rdd.map(lambda x: (x,1))


#We are now grouping the elements
dict_grp=dict_rdd.groupByKey()

#Applying the mapValues function here because we need to get a sum of the values to get the total count. Here the key is the grocery product and the value is the number of times it is occuring.
#Here we only want to transform the values(take the sum) but do not want our keys to be affected hence using mapvalues

grp_val = dict_grp.mapValues(sum).map(lambda x: (x[1],x[0])).sortByKey(False)


f=open('/Users/shubh/Documents/Truata_Output/Task_1/out_1_3.txt','w')
for i in grp_val.collect():
    f.write(str(i[1]) + '    ')
    f.write(str(i[0]))
    f.write("\n")

f.close()

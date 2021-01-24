from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, IntegerType,StructType, StructField,FloatType,DecimalType
from pyspark.sql.functions import *

#Build and initiate the spark session for leveraging spark
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("csv_processing") \
        .getOrCreate()

#Building the spark context
sc=spark.sparkContext

#Providing the location for the file to be processed
file_path = r"C:\Users\shubh\Documents\Truata_Tasks\Parquet_files"

#Loading the parquet file in the dataframe
a = spark.read.parquet(file_path)

#Task 2

total_count=a.count()

max_price = a.agg({"price": "max"}).collect()[0][0] #Getting the max of all the prices

min_price=a.agg({"price": "min"}).collect()[0][0] #Getting the min of all the prices


schema = StructType([StructField('min_price', FloatType(), True),
                     StructField('max_price', FloatType(), True),
                     StructField('total_count', IntegerType(), True)]) #Defining a schema to apply to the dataframe containing values

final_df_values=[(min_price,max_price,total_count)]


final_df=spark.createDataFrame(final_df_values,schema) #Creation of data frame

final_path=r"file:////Users/shubh/Documents/Truata_Output/Task_2/"

# We can write the data into a csv file using the below approach but since I was facing issues within my local windows laptop I had to follow the manual approach of writing to the file
# final_df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(final_path)

final_df_values_l=[min_price,max_price,total_count]

f=open('/Users/shubh/Documents/Truata_Output/Task_2/out_2_2.txt','w')
f.write('Min_price    Max_price      Total_count')
f.write("\n")
for ele in final_df_values_l:
    f.write(str(ele)+ '   ')
f.close()

#Task 3

filter_records=a.select('bedrooms','bathrooms').filter((trim('price') > lit(5000)) & (trim('review_scores_value') == lit(10.0)))

avg_bedrooms = filter_records.agg({"bedrooms": "avg"}).collect()[0][0]

avg_bathrooms=filter_records.agg({"bathrooms": "avg"}).collect()[0][0]

schema_2 = StructType([StructField('average_bedroom', FloatType(), True),
                     StructField('average_bathroom', FloatType(), True)])

avg_value=[(avg_bedrooms,avg_bathrooms)]
avg_value_l=[avg_bedrooms,avg_bathrooms]
final_avg=spark.createDataFrame(data=avg_value,schema=schema_2)

final_avg_path=r"file:////Users/shubh/Documents/Truata_Output/Task_2/"

#We can write the data into a csv file using the below approach but since I was facing issues within my local windows laptop I had to follow the manual approach of writing to the file
# final_avg.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(final_avg_path)


f=open('/Users/shubh/Documents/Truata_Output/Task_2/out_2_3.txt','w')
f.write('Avg Bedrooms   Avg Bathrooms')
f.write("\n")
f.write("\n")
for ele in avg_value_l:
    f.write(str(ele)+ '   ')


#Task 4

lowest_price = a.agg({"price": "min"}).collect()[0][0]

highest_rating=a.agg({"review_scores_rating": "max"}).collect()[0][0]


acc=a.select('accommodates').filter((trim('price') == lit(lowest_price)) & (trim('review_scores_rating') == lit(highest_rating)))

final_val=acc.collect()[0][0]

f=open('/Users/shubh/Documents/Truata_Output/Task_2/out_2_4.txt','w')

f.write(str(final_val))

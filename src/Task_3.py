from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, IntegerType,StructType, StructField,FloatType,DecimalType
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer


#Build and initiate the spark session for leveraging spark
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("ml") \
        .getOrCreate()

#Building the spark context
sc=spark.sparkContext

#Providing the location for the file to be processed
file_path = r"C:\Users\shubh\Documents\Truata_Tasks\iris.csv"

#Defining the schema for the file to be read and the schema which will be applied to it
schema = StructType([StructField('sepal_length', FloatType(), True),
                     StructField('sepal_width', FloatType(), True),
                     StructField('petal_length', FloatType(), True),
                     StructField('petal_width', FloatType(), True),
                     StructField('species', StringType(), True)])

#Loading the parquet file in the dataframe
a = spark.read.csv(file_path,header=False,schema=schema)

#Defininig the list of input columns
input=['sepal_length','sepal_width','petal_length','petal_width']

#The idea behind vector assembler is to have all the defining characteristics within one single column to help with the training later
Vec_assembler=VectorAssembler(inputCols=input,outputCol='features')

#The features column will now contain values from all the input columns identified
assemble_apply=Vec_assembler.transform(a)

# assemble_apply.show(10)

#Since we have integrated the defining features into the features above we don't require them any further so we can remove them from our df
assembly_final=assemble_apply.drop('sepal_length','sepal_width','petal_length','petal_width')

# assembly_final.show(5)

#Adding a labelindex for our defining class which in this case is species.
#We would get the label index as 0 1 2 depending on the frequency of occurence with 0 being awarded to the highest occuring species
label=StringIndexer(inputCol='species',outputCol='label')

si_dataset_fit=label.fit(assembly_final).transform(assembly_final)
# si_dataset_fit.show(5)

#We are now dividing our data set into two parts. the training and the test to see how accurate our model is
#Going with the 80-20 split here. 80 for training and 20 for testing
train_data,test_data=si_dataset_fit.randomSplit([0.8,0.2])

#Using logistic regression

rg=0.03 #Can be changed depending on what value yield more accurate results.

lr =LogisticRegression(featuresCol='features',labelCol='label',regParam=rg)
model = lr.fit(train_data) #Fitting the data for linear regression

#Actually seeing how it is performing using by testing it using the test data
prediction=model.transform(test_data)
prediction.show()

#This is for getting an idea of how accurate our overall model is
evaluator =MulticlassClassificationEvaluator(metricName='accuracy')
accuracy = evaluator.evaluate(prediction)

print(f'Predicted accuracy is {accuracy}')

#Applying the above to the test data provided in the question

pred_data = spark.createDataFrame(
 [(5.1, 3.5, 1.4, 0.2),
 (6.2, 3.4, 5.4, 2.3)],
 ["sepal_length", "sepal_width", "petal_length", "petal_width"])

#Once again applying the vectorassembler to have all the defining characteristics under a single column
Vec_assembler=VectorAssembler(inputCols=input,outputCol='features')

assemble_apply_test=Vec_assembler.transform(pred_data)

assemble_apply_test.show(10)

#Prediction of the dataframe in question
prediction=model.transform(assemble_apply_test)
prediction.show()

output=prediction.select("sepal_length", "sepal_width", "petal_length", "petal_width","prediction")

#We can write the data into a csv file using the below approach but since I was facing issues within my local windows laptop I had to follow the manual approach of writing to the file
# output.write.mode('overwrite').csv('prediction.csv')

f=open('/Users/shubh/Documents/Truata_Output/Task_3/out_3_1.txt','w')
f.write("sepal_length  sepal_width  petal_length  petal_width   prediction ")
f.write("\n")
for i in output.collect():
    f.write(str(i[0]) + ' ' + str(i[1]) + ' ' + str(i[2]) + ' ' + str(i[3]) + ' ' + str(i[4]))
    f.write("\n")

f.close()

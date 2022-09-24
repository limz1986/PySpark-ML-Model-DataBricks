

#%% Libraries 
# Think preprocessing of distributed systems for huge amts of data
# Mainly use in big data using Mlib
# 
from datetime import datetime, date
import pandas as pd
import pyspark
from pyspark.sql import Row
import pandas as pd
from pyspark.sql import SparkSession


#%% Spark Session 1_testing
spark=SparkSession.builder.appName('Practise1').getOrCreate()
pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})



#Create a sparkdf from a pandas df
df = spark.createDataFrame(pandas_df)
df = spark.read.option('header','true')

df.printSchema()
df.head()
df.show(1)
df.describe().show()


#%% Session 2

#Start a spark session 
from pyspark.sql import SparkSession
type(pd.read_csv('C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test1.csv'))


spark=SparkSession.builder.appName('Practise').getOrCreate()
spark



df_pyspark=spark.read.csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test1.csv')
#('header','true') will make the first column the header
df_pyspark=spark.read.option('header','true').csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test1.csv')

type(df_pyspark)
df_pyspark
#See columns
df_pyspark.printSchema()
df_pyspark.head()

#%% Session 3

# Objectives
# - PySpark Dataframe 
# - Reading The Dataset
# - Checking the Datatypes of the Column(Schema)
# - Selecting Columns And Indexing
# - Check Describe option similar to Pandas
# - Adding Columns
# - Dropping columns
# - Renaming Columns


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Dataframe').getOrCreate()
spark

## read the dataset
df_pyspark=spark.read.option('header','true').csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test1.csv',inferSchema=True)

### Check the schema
df_pyspark.printSchema()
# InferSchema = True results in making letting the programme infer the inferred Schema
df_pyspark=spark.read.csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test1.csv',header=True,inferSchema=True)
df_pyspark.show()

### Check the schema
df_pyspark.printSchema()
df_pyspark.columns

type(df_pyspark)
df_pyspark.head(3)

df_pyspark.show()

df_pyspark.select(['Name','Experience']).show()

df_pyspark['Name']

df_pyspark.dtypes

df_pyspark.describe().show()

### Adding Columns in data frame
df_pyspark=df_pyspark.withColumn('Experience After 2 year',df_pyspark['Experience']+2)
df_pyspark.show()

### Drop the columns
df_pyspark=df_pyspark.drop('Experience After 2 year')
df_pyspark.show()

### Rename the columns
df_pyspark.withColumnRenamed('Name','New Name').show()


#%% Session 4

# ### Pyspark Handling Missing Values
# - Dropping Columns
# - Dropping Rows
# - Various Parameter In Dropping functionalities
# - Handling Missing values by Mean, MEdian And Mode


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practise').getOrCreate()

df_pyspark=spark.read.csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test2.csv',header=True,inferSchema=True)

df_pyspark.printSchema()
df_pyspark.show()

##drop the columns
df_pyspark.drop('Name').show()
df_pyspark.show()

df_pyspark.na.drop().show()

### any==how
df_pyspark.na.drop(how="any").show()

##threshold
df_pyspark.na.drop(how="any",thresh=3).show()

##Subset ie only dropping values from column experience
df_pyspark.na.drop(how="any",subset=['Age']).show()

### Filling the Missing Value
df_pyspark.na.fill(' blank ').show()
df_pyspark.na.fill('Missing Values',['Experience','age']).show()

df_pyspark.show()
df_pyspark.printSchema()

from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")


# Add imputation cols to df
imputer.fit(df_pyspark).transform(df_pyspark).show()

#%% Session 5

# ### Pyspark Dataframes
# - Filter Operation
# - &,|,==
# - ~

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('dataframe').getOrCreate()

df_pyspark=spark.read.csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test1.csv',header=True,inferSchema=True)
df_pyspark.show()

# ### Filter Operations

### Salary of the people less than or equal to 20000
df_pyspark.filter("Salary<=20000").show()

df_pyspark.filter("Salary<=20000").select(['Name','age']).show()

df_pyspark.filter(df_pyspark['Salary']<=20000).show()


df_pyspark.filter((df_pyspark['Salary']<=20000) | 
                  (df_pyspark['Salary']>=15000)).show()

# not condition
df_pyspark.filter(~(df_pyspark['Salary']<=20000)).show()


#%% Session 5

# ### Pyspark GroupBy And Aggregate Functions
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Agg').getOrCreate()
spark

df_pyspark=spark.read.csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test3.csv',header=True,inferSchema=True)
df_pyspark.show()
df_pyspark.printSchema()


## Groupby
### Grouped to find the maximum salary
df_pyspark.groupBy('Name').sum().show()
df_pyspark.groupBy('Name').avg().show()


### Groupby Departmernts  which gives maximum salary
df_pyspark.groupBy('Departments').sum().show()

df_pyspark.groupBy('Departments').mean().show()
df_pyspark.groupBy('Departments').count().show()
df_pyspark.agg({'Salary':'sum'}).show()



#%% Session 6

# ### Examples Of Pyspark ML
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Missing').getOrCreate()

## Read The dataset
training = spark.read.csv(r'C:/Users/65904/Desktop/Learning Python/Pyspark-With-Python-main/test1.csv',header=True,inferSchema=True)
training.show()
training.printSchema()

training.columns

# [Age,Experience]----> new feature--->independent feature
from pyspark.ml.feature import VectorAssembler
featureassembler=VectorAssembler(inputCols=["age","Experience"],outputCol="Independent Features")

output=featureassembler.transform(training)

output.show()
output.columns

finalized_data=output.select("Independent Features","Salary")
finalized_data.show()


from pyspark.ml.regression import LinearRegression
##train test split
train_data,test_data=finalized_data.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor=regressor.fit(train_data)

### Coefficients
regressor.coefficients

### Intercepts
regressor.intercept

### Prediction
pred_results=regressor.evaluate(test_data)

pred_results.predictions.show()

pred_results.meanAbsoluteError,pred_results.meanSquaredError

#%% Session 6


# This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.


# File location and type
file_location = "/FileStore/tables/tips.csv"
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df =spark.read.csv(file_location,header=True,inferSchema=True)
df.show()


df.printSchema()
df.columns

### Handling Categorical Features
from pyspark.ml.feature import StringIndexer

df.show()

indexer=StringIndexer(inputCol="sex",outputCol="sex_indexed")
df_r=indexer.fit(df).transform(df)
df_r.show()


indexer=StringIndexer(inputCols=["smoker","day","time"],outputCols=["smoker_indexed","day_indexed",
                                                                  "time_index"])
df_r=indexer.fit(df_r).transform(df_r)
df_r.show()

df_r.columns

from pyspark.ml.feature import VectorAssembler
featureassembler=VectorAssembler(inputCols=['tip','size','sex_indexed','smoker_indexed','day_indexed',
                          'time_index'],outputCol="Independent Features")
output=featureassembler.transform(df_r)


output.select('Independent Features').show()
output.show()

finalized_data=output.select("Independent Features","total_bill")
finalized_data.show()

from pyspark.ml.regression import LinearRegression
##train test split
train_data,test_data=finalized_data.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol='Independent Features', labelCol='total_bill')
regressor=regressor.fit(train_data)


regressor.coefficients
regressor.intercept

### Predictions
pred_results=regressor.evaluate(test_data)


## Final comparison
pred_results.predictions.show()


### PErformance Metrics
pred_results.r2,pred_results.meanAbsoluteError,pred_results.meanSquaredError




#%% Session 8


Random Forest Classifier Example.

# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("RandomForestClassifierExample")\
        .getOrCreate()

    # $example on$
    # Load and parse the data file, converting it to a DataFrame.
    data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

    # Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer =\
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

    # Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                   labels=labelIndexer.labels)

    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g" % (1.0 - accuracy))

    rfModel = model.stages[2]
    print(rfModel)  # summary only
    # $example off$

    spark.stop()



#%% Session 9 
























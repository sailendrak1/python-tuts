
from pyspark.sql import SparkSession

sc = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('example_spark')\
    .getOrCreate()

mydata=sc.read.format("csv").option("header","true").load("/original.csv")
mydata.show()

#clean DATA with new column

from pyspark.sql.functions import *
mydata2=mydata.withColumn("clean_city",when(mydata.City.isNull(),"UNKNOWN").otherwise(mydata.City))

#fiter job title that has nulls

mydata2 = mydata2.filter(mydata2.JobTitile.ISNOTNULL())
mydata2.show()

#clear $ for salary and convert to float datatype

mydata2 = mydata2.withColumn("clean_salary",mydata2.Salary.substr(2,100).cast('float'))
mydata2.show()

#getting the average Salary based on gender

import pyspark.sql.functions as sqlfun
genders = mydata2.groupBy('gender').agg(sqlfun.avg('clean_salary').alias('avg_sal'))
genders.show()

fem_sal_data = mydata2.withColumn("female_sal",when(mydata2.gender=="Female",mydata2.clean_salary).otherwise(lit(0)))
fem_sal_data.show()

male_sal_data=fem_sal_data.withColumn("male_salary",when(mydata2.gender=="Male",mydata2.clean_salary).otherwise(lit(0)))
male_sal_data.show()

df2 = male_sal_data.groupBy("JobTitle").agg(sqlfun.avg('female_sal').alias('female_avg_sal'),sqlfun.avg('male_salary').alias('male_avg_sal'))
df2.show()

df_final = df2.withColumn('delta',df2.male_avg_sal - df2.female_avg_sal)
df_final.show()


#get highest salary city wise
df_city=mydata2.groupBy('City').agg(avg('clean_salary').alias('city_sal'))

df3 = df_city.sort(col('city_sal').desc())
df3.show


df = sc.read.csv('/content/sample_data/original.csv',header='true')

df.dtypes

[('id', 'string'),
 ('first_name', 'string'),
 ('last_name', 'string'),
 ('gender', 'string'),
 ('City', 'string'),
 ('JobTitle', 'string'),
 ('Salary', 'string'),
 ('Latitude', 'string'),
 ('Longitude', 'string')]
 
#defining schema for TABLE

from pyspark.sql.types import *

myscehma = StructType([
StructField("Id",IntegerType()),
StructField("First_Name",StringType()),
StructField("Last_Name",StringType()),
StructField("gender",StringType()),
StructField("City",StringType()),
StructField("Job_Title",StringType()),
StructField("Salary",StringField()),
StructField("Latitude",FloatType()),
StructField("Longitude",FloatType())
]
)

df = sc.read.csv('/content/sample_data/original.csv',header='true',schema=myscehma)

df.dtypes

[('Id', 'int'),
 ('First_Name', 'string'),
 ('Last_Name', 'string'),
 ('gender', 'string'),
 ('City', 'string'),
 ('Job_Title', 'string'),
 ('Salary', 'string'),
 ('Latitude', 'float'),
 ('Longitude', 'float')]
 
 #get first 10 rows
 df.head(10)
 
 #get first row
 df.first()
 
 df.distinct().count()
 
 df.describe().show()
 
 
 df.columns
 
 #clear all null rows
 
 df2 = df.na.drop()
 df2.show()
 
df_select = df.select('First_Name','gender')
df_select.show()

df3 = df.withColumnRenamed('First_name','FN')
df3.show()

df3=df.filter(df.First_Name.startswith('end'))

df3.show()

df4 = df.filter(df.Id.between(3,5))
df4.show()

df4 = df.filter(df.First_Name.like('%ND%') & df.City.isin('New'))
df4.show()

df.createOrReplaceTempView('tmpTab')
df5=sc.sql("select * from tmpTab")
df5.show()
qry = sc.sql("select concat(First_Name,' ',Last_Name) as Name from tmpTab")
qry.show()

df4=df.withColumn('MorF',when(df.gender=='Male','YES').otherwise('NO'))
df4.show()

#df4.write.csv('myfile.csv')
df4.write.json('myjsonfile.json')
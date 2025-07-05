
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
df3.show()
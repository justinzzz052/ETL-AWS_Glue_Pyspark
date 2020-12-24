#<Source Code — — — Begins>
import sys
import datetime
import json
from collections import Iterable, OrderedDict
from itertools import product
import logging
import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import array, ArrayType, IntegerType, NullType, StringType, StructType
from pyspark.sql.functions import *
###################GLUE import##############

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.transforms import *

#### ###creating spark and gluecontext ###############

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

#### ### parameters ###########
glue_db = 'video_data_db'
glue_tbl = 'video_data_csv'
s3_write_path = 's3://glue-1-justin052/output/'

print('Job Execution Started…')

## ###Creating glue dynamic frame from the catalog ###########
ds = glueContext.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)

##### ##Creating spark data frame from the glue context ########

####################################
## Extract data ##
####################################

print('Extraction Job Execution Started…')

df = ds.toDF()
df.show()

####################################
## Transform data ##
####################################
print('Transformation Job Execution Started…')

df = df.withColumn('Splited_VideoTitle', lit(split(df['videotitle'], '\|'))) \
        .withColumn('colCount',lit(size('Splited_VideoTitle'))) \
        .withColumn('DimSite',lit(col('Splited_VideoTitle')[0]))\
        .withColumn('DimVideo',lit(element_at(col('Splited_VideoTitle'),-1))) \
        .withColumn('Date',lit(df.datetime.cast('date')))\
        .withColumn('Year',lit(year('DateTime')))\
        .withColumn('Quarter',lit(quarter('datetime')))\
        .withColumn('Month',lit(month('datetime')))\
        .withColumn('Week',lit(date_format('datetime','EEEE')))\
        .withColumn('day',lit(dayofmonth('datetime')))

# Create the columns on the DataFrame to classify items
df = df.withColumn('DimPlatform',when(df.DimSite.contains('iPhone'),'Iphone')\
                                .when(df.DimSite.contains('Android'),'Android')\
                                .when(df.DimSite.contains('iPad'),'Ipad')\
                                .otherwise('Desktop'))
                                
# Remove any rows containing fewer than 1 fields
# Remove any rows NOT containing 206
ds_df2 = df.filter(size('Splited_VideoTitle')>1) \
                .filter(df.events.contains('206'))

# # select could have csv format 
# ds_df2 = df.select('events')


####################################
## Load data ##
####################################                                     
print('Loading Job Execution Started…')

ds_df2 =ds_df2.repartition(1)

#convert back to dynamic frame

datasource0 = DynamicFrame.fromDF(ds_df2, glueContext, 'datasource0')

#write data back to s3

# datasink2 = glueContext.write_dynamic_frame.from_options(
#     frame = datasource0, 
#     connection_type = 's3', 
#     connection_options = {'path': s3_write_path},
#     format = 'csv')


glueContext.write_dynamic_frame.from_options(
    frame = datasource0,
    connection_type = 's3',
    connection_options = {
        'path':s3_write_path,
        },
        format = 'json'
    )

job.commit()
print('Job finished')
#<Source Code — — — Ends>
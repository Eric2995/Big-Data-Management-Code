import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col, sqrt
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import explode,sum
from pyspark.sql.functions import split, col, regexp_replace
import sys
from pyspark.sql.functions import udf
from pyproj import Transformer
from pyspark.sql.types import StringType
from pyspark.sql.functions import expr

sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()
supermarket_df = spark.read.csv("nyc_supermarkets.csv", header=True)

# Read the CBG centroids data
cbg_df = spark.read.csv("nyc_cbg_centroids.csv", header=True)
# Read the weekly patterns data
weekly_patterns_df = spark.read.csv("/shared/CUSP-GX-6002/data/weekly-patterns-nyc-2019-2020/part-*", header=True)

# Filter the visits in the weekly patterns data using the supermarket data
filtered_patterns_df = weekly_patterns_df.join(supermarket_df, weekly_patterns_df.placekey == supermarket_df.safegraph_placekey, "inner")
def transform_coordinates(latitude, longitude):
    transformer = Transformer.from_crs('EPSG:4326', 'EPSG:2263', always_xy=True)
    projected_coordinates = transformer.transform(longitude, latitude)
    x = projected_coordinates[0]
    y = projected_coordinates[1]
    return f"{x},{y}"

transform_udf = udf(transform_coordinates, StringType())
filtered_patterns_df = filtered_patterns_df.filter(
    (col('poi_cbg').startswith('36061')) |
    (col('poi_cbg').startswith('36005')) |
    (col('poi_cbg').startswith('36047')) |
    (col('poi_cbg').startswith('36081')) |
    (col('poi_cbg').startswith('36085'))
)
condition1 = ((col("date_range_start") >= "2019-03-01") & (col("date_range_start") < "2019-04-01")) | ((col("date_range_end") >= "2019-03-01") & (col("date_range_end") < "2019-04-01"))
condition2 = ((col("date_range_start") >= "2019-10-01") & (col("date_range_start") < "2019-11-01")) | ((col("date_range_end") >= "2019-10-01") & (col("date_range_end") < "2019-11-01"))
condition3 = ((col("date_range_start") >= "2020-03-01") & (col("date_range_start") < "2020-04-01")) | ((col("date_range_end") >= "2020-03") & (col("date_range_end") < "2020-04-01"))
condition4 = ((col("date_range_start") >= "2020-10-01") & (col("date_range_start") < "2020-11-01")) | ((col("date_range_end") >= "2020-10-01") & (col("date_range_end") < "2020-11-01"))
df1 = filtered_patterns_df.filter(condition1)
df2 = filtered_patterns_df.filter(condition2)
df3 = filtered_patterns_df.filter(condition3)
df4 = filtered_patterns_df.filter(condition4)
split_df1 = df1.withColumn('visitor_home_cbgs_array', split(df1['visitor_home_cbgs'], ','))
split_df2 = df2.withColumn('visitor_home_cbgs_array', split(df2['visitor_home_cbgs'], ','))
split_df3 = df3.withColumn('visitor_home_cbgs_array', split(df3['visitor_home_cbgs'], ','))
split_df4 = df4.withColumn('visitor_home_cbgs_array', split(df4['visitor_home_cbgs'], ','))

exploded_df1 = split_df1.select('poi_cbg', explode(split_df1['visitor_home_cbgs_array']).alias('visitor_home_cbgs'))
exploded_df2 = split_df2.select('poi_cbg', explode(split_df2['visitor_home_cbgs_array']).alias('visitor_home_cbgs'))
exploded_df3 = split_df3.select('poi_cbg', explode(split_df3['visitor_home_cbgs_array']).alias('visitor_home_cbgs'))
exploded_df4 = split_df4.select('poi_cbg', explode(split_df4['visitor_home_cbgs_array']).alias('visitor_home_cbgs'))
split_df1 = exploded_df1.withColumn('split_visitor_home_cbgs', split(col('visitor_home_cbgs'), ':'))
split_df2 = exploded_df2.withColumn('split_visitor_home_cbgs', split(col('visitor_home_cbgs'), ':'))
split_df3 = exploded_df3.withColumn('split_visitor_home_cbgs', split(col('visitor_home_cbgs'), ':'))
split_df4 = exploded_df4.withColumn('split_visitor_home_cbgs', split(col('visitor_home_cbgs'), ':'))

extracted_df1 = split_df1.withColumn('home_cbg', col('split_visitor_home_cbgs')[0]) \
                      .withColumn('count', col('split_visitor_home_cbgs')[1])
extracted_df2 = split_df2.withColumn('home_cbg', col('split_visitor_home_cbgs')[0]) \
                      .withColumn('count', col('split_visitor_home_cbgs')[1])
extracted_df3 = split_df3.withColumn('home_cbg', col('split_visitor_home_cbgs')[0]) \
                      .withColumn('count', col('split_visitor_home_cbgs')[1])
extracted_df4 = split_df4.withColumn('home_cbg', col('split_visitor_home_cbgs')[0]) \
                      .withColumn('count', col('split_visitor_home_cbgs')[1])
filtered_df1 = extracted_df1.withColumn('home_cbg', regexp_replace(col('home_cbg'), '[^0-9]', '')) \
                          .withColumn('count', regexp_replace(col('count'), '[^0-9]', ''))
filtered_df2 = extracted_df2.withColumn('home_cbg', regexp_replace(col('home_cbg'), '[^0-9]', '')) \
                          .withColumn('count', regexp_replace(col('count'), '[^0-9]', ''))
filtered_df3 = extracted_df3.withColumn('home_cbg', regexp_replace(col('home_cbg'), '[^0-9]', '')) \
                          .withColumn('count', regexp_replace(col('count'), '[^0-9]', ''))
filtered_df4 = extracted_df4.withColumn('home_cbg', regexp_replace(col('home_cbg'), '[^0-9]', '')) \
                          .withColumn('count', regexp_replace(col('count'), '[^0-9]', ''))
filtered_df1 = filtered_df1.dropna(subset=['count'])
filtered_df2 = filtered_df2.dropna(subset=['count'])
filtered_df3 = filtered_df3.dropna(subset=['count'])
filtered_df4 = filtered_df4.dropna(subset=['count'])
join_df1=filtered_df1.join(cbg_df,filtered_df1.home_cbg == cbg_df.cbg_fips, "inner")
join_df1 = join_df1.withColumnRenamed("cbg_fips", "home_cbg1")
join_df1 = join_df1.withColumnRenamed("latitude", "latitude1")
join_df1 = join_df1.withColumnRenamed("longitude", "longitude1")
join_df1=join_df1.join(cbg_df,filtered_df1.poi_cbg == cbg_df.cbg_fips, "inner")
join_df1 = join_df1.withColumnRenamed("latitude", "latitude2")
join_df1 = join_df1.withColumnRenamed("longitude", "longitude2")

join_df2=filtered_df2.join(cbg_df,filtered_df2.home_cbg == cbg_df.cbg_fips, "inner")
join_df2 = join_df2.withColumnRenamed("cbg_fips", "home_cbg1")
join_df2 = join_df2.withColumnRenamed("latitude", "latitude1")
join_df2 = join_df2.withColumnRenamed("longitude", "longitude1")
join_df2=join_df2.join(cbg_df,filtered_df2.poi_cbg == cbg_df.cbg_fips, "inner")
join_df2 = join_df2.withColumnRenamed("latitude", "latitude2")
join_df2 = join_df2.withColumnRenamed("longitude", "longitude2")

join_df3=filtered_df3.join(cbg_df,filtered_df3.home_cbg == cbg_df.cbg_fips, "inner")
join_df3 = join_df3.withColumnRenamed("cbg_fips", "home_cbg1")
join_df3 = join_df3.withColumnRenamed("latitude", "latitude1")
join_df3 = join_df3.withColumnRenamed("longitude", "longitude1")
join_df3=join_df3.join(cbg_df,filtered_df3.poi_cbg == cbg_df.cbg_fips, "inner")
join_df3 = join_df3.withColumnRenamed("latitude", "latitude2")
join_df3 = join_df3.withColumnRenamed("longitude", "longitude2")

join_df4=filtered_df4.join(cbg_df,filtered_df4.home_cbg == cbg_df.cbg_fips, "inner")
join_df4 = join_df4.withColumnRenamed("cbg_fips", "home_cbg1")
join_df4 = join_df4.withColumnRenamed("latitude", "latitude1")
join_df4 = join_df4.withColumnRenamed("longitude", "longitude1")
join_df4=join_df4.join(cbg_df,filtered_df4.poi_cbg == cbg_df.cbg_fips, "inner")
join_df4 = join_df4.withColumnRenamed("latitude", "latitude2")
join_df4 = join_df4.withColumnRenamed("longitude", "longitude2")
join_df1 = join_df1.withColumn('projected_coordinates', transform_udf('latitude1', 'longitude1'))\
               .withColumn('x1', split('projected_coordinates', ',')[0].cast('float'))\
               .withColumn('y1', split('projected_coordinates', ',')[1].cast('float'))\
               .drop('projected_coordinates')
join_df1 = join_df1.withColumn('projected_coordinates', transform_udf('latitude2', 'longitude2'))\
               .withColumn('x2', split('projected_coordinates', ',')[0].cast('float'))\
               .withColumn('y2', split('projected_coordinates', ',')[1].cast('float'))\
               .drop('projected_coordinates')
join_df2 = join_df2.withColumn('projected_coordinates', transform_udf('latitude1', 'longitude1'))\
               .withColumn('x1', split('projected_coordinates', ',')[0].cast('float'))\
               .withColumn('y1', split('projected_coordinates', ',')[1].cast('float'))\
               .drop('projected_coordinates')
join_df2 = join_df2.withColumn('projected_coordinates', transform_udf('latitude2', 'longitude2'))\
               .withColumn('x2', split('projected_coordinates', ',')[0].cast('float'))\
               .withColumn('y2', split('projected_coordinates', ',')[1].cast('float'))\
               .drop('projected_coordinates')

join_df3 = join_df3.withColumn('projected_coordinates', transform_udf('latitude1', 'longitude1'))\
              .withColumn('x1', split('projected_coordinates', ',')[0].cast('float'))\
              .withColumn('y1', split('projected_coordinates', ',')[1].cast('float'))\
              .drop('projected_coordinates')
join_df3 = join_df3.withColumn('projected_coordinates', transform_udf('latitude2', 'longitude2'))\
               .withColumn('x2', split('projected_coordinates', ',')[0].cast('float'))\
               .withColumn('y2', split('projected_coordinates', ',')[1].cast('float'))\
               .drop('projected_coordinates')

join_df4 = join_df4.withColumn('projected_coordinates', transform_udf('latitude1', 'longitude1'))\
               .withColumn('x1', split('projected_coordinates', ',')[0].cast('float'))\
               .withColumn('y1', split('projected_coordinates', ',')[1].cast('float'))\
               .drop('projected_coordinates')
join_df4 = join_df4.withColumn('projected_coordinates', transform_udf('latitude2', 'longitude2'))\
               .withColumn('x2', split('projected_coordinates', ',')[0].cast('float'))\
               .withColumn('y2', split('projected_coordinates', ',')[1].cast('float'))\
               .drop('projected_coordinates')
join_df1 = join_df1.withColumn('distance', sqrt((col('x2') - col('x1'))**2 + (col('y2') - col('y1'))**2))
join_df1 = join_df1.withColumn('distance_times_count', expr('distance * count'))
join_df1 = join_df1.select('home_cbg1', 'distance_times_count','count')

join_df2 = join_df2.withColumn('distance', sqrt((col('x2') - col('x1'))**2 + (col('y2') - col('y1'))**2))
join_df2 = join_df2.withColumn('distance_times_count', expr('distance * count'))
join_df2 = join_df2.select('home_cbg1', 'distance_times_count','count')

join_df3 = join_df3.withColumn('distance', sqrt((col('x2') - col('x1'))**2 + (col('y2') - col('y1'))**2))
join_df3 = join_df3.withColumn('distance_times_count', expr('distance * count'))
join_df3 = join_df3.select('home_cbg1', 'distance_times_count','count')

join_df4 = join_df4.withColumn('distance', sqrt((col('x2') - col('x1'))**2 + (col('y2') - col('y1'))**2))
join_df4 = join_df4.withColumn('distance_times_count', expr('distance * count'))
join_df4 = join_df4.select('home_cbg1', 'distance_times_count','count')

grouped_df1 = join_df1.groupBy('home_cbg1').agg(sum('distance_times_count').alias('total_distance'), sum('count').alias('total_count'))
grouped_df1 = grouped_df1.withColumn('distance_per_count', col('total_distance') / (col('total_count') * 5280))
grouped_df1 = grouped_df1.select('home_cbg1','distance_per_count')

grouped_df2 = join_df2.groupBy('home_cbg1').agg(sum('distance_times_count').alias('total_distance'), sum('count').alias('total_count'))
grouped_df2 = grouped_df2.withColumn('distance_per_count', col('total_distance') / (col('total_count') * 5280))
grouped_df2 = grouped_df2.select('home_cbg1','distance_per_count')

grouped_df3 = join_df4.groupBy('home_cbg1').agg(sum('distance_times_count').alias('total_distance'), sum('count').alias('total_count'))
grouped_df3 = grouped_df3.withColumn('distance_per_count', col('total_distance') / (col('total_count') * 5280))
grouped_df3 = grouped_df3.select('home_cbg1','distance_per_count')

grouped_df4 = join_df4.groupBy('home_cbg1').agg(sum('distance_times_count').alias('total_distance'), sum('count').alias('total_count'))
grouped_df4 = grouped_df4.withColumn('distance_per_count', col('total_distance') / (col('total_count') * 5280))
grouped_df4 = grouped_df4.select('home_cbg1','distance_per_count')
output=cbg_df.select('cbg_fips')
output=output.orderBy('cbg_fips')
output=output.join(grouped_df1,output.cbg_fips == grouped_df1.home_cbg1, "left_outer").drop('home_cbg1')
output = output.withColumnRenamed('distance_per_count', '2019-03')
output=output.join(grouped_df2,output.cbg_fips == grouped_df2.home_cbg1, "left_outer").drop('home_cbg1')
output = output.withColumnRenamed('distance_per_count', '2019-10')
output=output.join(grouped_df3,output.cbg_fips == grouped_df3.home_cbg1, "left_outer").drop('home_cbg1')
output = output.withColumnRenamed('distance_per_count', '2020-03')
output=output.join(grouped_df4,output.cbg_fips == grouped_df4.home_cbg1, "left_outer").drop('home_cbg1')
output = output.withColumnRenamed('distance_per_count', '2020-10')
output_folder = sys.argv[1]
output=output.orderBy("cbg_fips")
output.write.format("csv").option("header", "true").mode("overwrite").save(output_folder)


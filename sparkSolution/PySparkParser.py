from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as f
import re
import sys

# spark-submit spark.py <input file> <output file> <url cluster> <sparku path cluster>

if len(sys.argv) == 0:
    path_file = '../data/freebase-head-100000000'
    output_file = 'outputs/third_iteration'
    sc = SparkSession.builder.master('local[*]').appName('IR Person entity, FREEBASE').config("spark.driver.memory","15g").getOrCreate()
    
elif len(sys.argv) == 4:
    path_file = sys.argv[1]
    output_file = sys.argv[2]
    url_cluster_master = sys.argv[3]
    spark_path = sys.argv[4]    
    sc = SparkSession.builder.master(url_cluster_master).appName('IR Person entity, FREEBASE').getOrCreate().config("spark.executor.uri", spark_path)
else:
    print("Wrong number of arguments.")
    exit()

re_name = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/type\.object\.name>\t\".*\"@en)'
re_person = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.person\..*>\t)'
re_dec_person = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.deceased_person\..*>\t)'

schema = StructType([StructField('id', StringType(), True),
                    StructField('predicate', StringType(), True),
                    StructField('value', StringType(), True, metadata = {"maxlength":2048})])

freebase = sc.sparkContext.textFile(path_file)
filtered_data = freebase \
    .filter(lambda x: re.search(re_name, x) or re.search(re_person, x) or re.search(re_dec_person, x)) \
    .distinct() \
    .map(lambda x: re.sub('(http\:\/\/rdf.freebase.com\/ns\/)|(\^\^.*\.)|(\@.*\.)|\<|\>|\"|(\t\.)', "", x)) \
    .map(lambda x: x.split('\t'))

data_reference = sc.sparkContext.textFile(path_file)
data_reference = freebase \
    .filter(lambda x: re.search(re_name, x)) \
    .distinct() \
    .map(lambda x: re.sub('(http\:\/\/rdf.freebase.com\/ns\/)|(\^\^.*\.)|(\@.*\.)|\<|\>|\"|(\t\.)',"",x)) \
    .map(lambda x: x.split('\t')) 

filtered_aliases = sc.createDataFrame(data_reference.filter(lambda x: "type.object.name" in x[1]), schema)

names = sc.createDataFrame(filtered_data.filter(lambda x: "type.object.name" in x[1]), schema)
births = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.date_of_birth" in x[1]), schema)
deaths = sc.createDataFrame(filtered_data.filter(lambda x: "people.deceased_person.date_of_death" in x[1]), schema)
nationality = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.nationality" in x[1]), schema)
height_meters = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.height_meters" in x[1]), schema)
weight_kg = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.weight" in x[1]), schema)
place_of_birth = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.place_of_birth" in x[1]), schema)
others = sc.createDataFrame(filtered_data.filter(lambda x: "people" in x[1] and "date_of_birth" not in x[1] and "date_of_death" not in x[1]), schema)

births = births.withColumn("note", f.lit(""))
deaths = deaths.withColumn("note", f.lit(""))

names.registerTempTable("names")
births.registerTempTable("births")
deaths.registerTempTable("deaths")
nationality.registerTempTable("nationality")
height_meters.registerTempTable("height_meters")
place_of_birth.registerTempTable("place_of_birth")
weight_kg.registerTempTable("weight_kg")
others.registerTempTable("others")

sql_context = SQLContext(sc.sparkContext)
first_iteration_people = sql_context.sql("""
    Select names.id as id, names.value as name,
    Case
        when births.value is not null then (cast(births.value as date)) 
        when deaths.value is not null and births.value is null then (cast(deaths.value as date) - 100*365)
        when deaths.value is null and births.value is null then ''
    end as birth,
    Case
        when deaths.value is not null then (cast(deaths.value as date))
        when births.value is not null and deaths.value is null then (cast(births.value as date) + 100*365)
        when deaths.value is null and births.value is null then ''
    end as death,
    ifnull(place_of_birth.value, '') as place_of_birth_ref,
    ROUND(ifnull(weight_kg.value, ''), 2) weight_kg,
    ifnull(nationality.value, '') as nationality_ref,
    ROUND(ifnull(height_meters.value, ''), 2) as height_meters,
    ifnull(births.note, 'Birthdate could not be valid.') as birth_validation,
    ifnull(deaths.note, 'Death date could not be valid..') as death_validation
    from names
    left join births on names.id = births.id
    left join deaths on names.id = deaths.id
    left join nationality on names.id = nationality.id
    left join height_meters on names.id = height_meters.id
    left join place_of_birth on names.id = place_of_birth.id
    left join weight_kg on names.id = weight_kg.id
    left join others on names.id = others.id
    where births.value is not null or deaths.value is not null or nationality.value is not null or height_meters.value is not null 
    or place_of_birth.value is not null or weight_kg.value is not null or others.value is not null
    """)

first_iteration_people = first_iteration_people.drop_duplicates().distinct()
first_iteration_people = first_iteration_people.withColumnRenamed("id", "id_person")

#first_iteration_people.repartition(1).write.mode("overwrite").format('com.databricks.spark.csv') \
#   .option("mapreduce.fileutputcommitter.marksuccessfuljobs", "false").save('outputs/first_iteration', header = 'true')

second_interation_nationality = first_iteration_people.join(filtered_aliases, first_iteration_people["nationality_ref"] == filtered_aliases["id"])
second_interation_nationality = second_interation_nationality.withColumnRenamed("value", "nationality")
second_interation_nationality = second_interation_nationality.drop("id", "predicate", "nationality_ref")


#second_interation_nationality.repartition(1).write.mode("overwrite").format('com.databricks.spark.csv') \
#    .option("mapreduce.fileutputcommitter.marksuccessfuljobs","false").save('outputs/second_iteration', header = 'true')

third_iteration_place_of_birth = second_interation_nationality.join(filtered_aliases, second_interation_nationality["place_of_birth_ref"] == filtered_aliases["id"])
third_iteration_place_of_birth = third_iteration_place_of_birth.withColumnRenamed("value", "place_of_birth")
third_iteration_place_of_birth = third_iteration_place_of_birth.drop("id", "predicate", "place_of_birth_ref")

third_iteration_place_of_birth.repartition(1).write.mode("overwrite").format('com.databricks.spark.csv') \
    .option("mapreduce.fileutputcommitter.marksuccessfuljobs","false").save(output_file, header = 'true')

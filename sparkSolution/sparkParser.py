from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as f
import re

import findspark
findspark.init()

re_name = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/type\.object\.name>\t\".*\"@en)'
re_person = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.person\..*>\t)'
re_dec_person = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.deceased_person\..*>\t)'
re_nationality = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/people\.person\..*>\t)'
re_alias = '(\/[gm]\..+\t<http:\/\/rdf\.freebase\.com\/ns\/common\.topic\.alias>\t\".*\"@en)'

pathFile = 'data/freebase-head-1000000.gz'

def sparkParser(filePath: str):
    sc = SparkSession.builder.master('local[*]').appName('IR Person entity, FREEBASE').getOrCreate()
    
    freebase = sc.sparkContext.textFile(filePath)
    filtered_data = freebase \
    .filter(lambda x: re.search(re_name,x) or re.search(re_person,x) or re.search(re_dec_person,x) or re.search(re_alias,x)) \
    .distinct() \
    .map(lambda x: re.sub('(http\:\/\/rdf.freebase.com\/ns\/)|(\^\^.*\.)|(\@.*\.)|\<|\>|\"|(\t\.)',"",x)) \
    .map(lambda x: x.split('\t')) 

    schema = StructType([StructField('subject', StringType(), True),
                    StructField('predicate', StringType(), True),
                    StructField('object', StringType(), True, metadata = {"maxlength":2048})])

    # names = sc.createDataFrame(filtered_data.filter(lambda x: "type.object.name" in x[1]), schema)
    # births = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.date_of_birth" in x[1]), schema)
    # deaths = sc.createDataFrame(filtered_data.filter(lambda x: "people.deceased_person.date_of_death" in x[1]), schema)
    # #nationality = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.nationality" in x[1]), schema)

    # aliases = sc.createDataFrame(filtered_data.filter(lambda x: "common.topic.alias" in x[1]), schema)
    # others = sc.createDataFrame(filtered_data.filter(lambda x: "people" in x[1] and "date_of_birth" not in x[1] and "date_of_death" not in x[1]), schema)

    # births = births.withColumn("note", f.lit(""))
    # deaths = deaths.withColumn("note", f.lit(""))
    # #nationality = nationality.withColumn("note", f.lit(""))

    # names.registerTempTable("names")
    # births.registerTempTable("births")
    # deaths.registerTempTable("deaths")
    # #nationality.registerTempTable("nationality")

    # aliases.registerTempTable("aliases")
    # others.registerTempTable("others")

    # sql_context = SQLContext(sc.sparkContext)

    # people = sql_context.sql("""
    #     select names.subject as id, names.object as name,
    #     case
    #         when births.object is not null then (cast(births.object as date)) 
    #         when deaths.object is not null and births.object is null then (cast(deaths.object as date) - 100*365)
    #         when deaths.object is null and births.object is null then ''
    #     end as birth,
    #     case
    #         when deaths.object is not null then (cast(deaths.object as date))
    #         when births.object is not null and deaths.object is null then (cast(births.object as date) + 100*365)
    #         when deaths.object is null and births.object is null then ''
    #     end as death,
    #     --ifnull(nationality.object, '') as nationality,
    #     ifnull(births.note, 'Datum narodenia nemusi byt spravny.') as b_note,
    #     ifnull(deaths.note, 'Datum umrtia nemusi byt spravny.') as d_note
    #     from names
    #     left join births on names.subject = births.subject
    #     left join deaths on names.subject = deaths.subject
    #     --left join nationality on names.subject = nationality.subject
    #     left join others on names.subject = others.subject
    #     where births.object is not null or deaths.object is not null --or nationality.object is not null 
    #     or others.object is not null
    #     """)


    names = sc.createDataFrame(filtered_data.filter(lambda x: "type.object.name" in x[1]), schema)
    aliases = sc.createDataFrame(filtered_data.filter(lambda x: "common.topic.alias" in x[1]), schema)
    births = sc.createDataFrame(filtered_data.filter(lambda x: "people.person.date_of_birth" in x[1]), schema)
    deaths = sc.createDataFrame(filtered_data.filter(lambda x: "people.deceased_person.date_of_death" in x[1]), schema)
    others = sc.createDataFrame(filtered_data.filter(lambda x: "people" in x[1] and "date_of_birth" not in x[1] and "date_of_death" not in x[1]), schema)
    
    births = births.withColumn("note", f.lit(""))
    deaths = deaths.withColumn("note", f.lit(""))

    names.registerTempTable("names")
    aliases.registerTempTable("aliases")
    births.registerTempTable("births")
    deaths.registerTempTable("deaths")
    others.registerTempTable("others")

    sql_context = SQLContext(sc.sparkContext)

    people = sql_context.sql("""
        select names.subject as id, names.object as name, 
        ifnull(aliases.object, '-') as alias,
        case
            when births.object is not null then (cast(births.object as date)) 
            when deaths.object is not null and births.object is null then (cast(deaths.object as date) - 100*365)
            when deaths.object is null and births.object is null then '-'
        end as birth,
        case
            when deaths.object is not null then (cast(deaths.object as date))
            when births.object is not null and deaths.object is null then (cast(births.object as date) + 100*365)
            when deaths.object is null and births.object is null then '-'
        end as death,
        ifnull(births.note, 'Datum narodenia nemusi byt spravny.') as b_note,
        ifnull(deaths.note, 'Datum umrtia nemusi byt spravny.') as d_note
        from names
        left join aliases on names.subject = aliases.subject
        left join births on names.subject = births.subject
        left join deaths on names.subject = deaths.subject
        left join others on names.subject = others.subject
        where births.object is not null or deaths.object is not null or others.object is not null
        """)

    people = people.distinct()

    # people = people.drop_duplicates()

    #tryDf = people.join(filtered_data,people.nationality ==  filtered_data.nationality,"inner") \
     #.show(truncate=False)


    # split data into n partitions and execute computations on the partitions in parallel
    people.repartition(1).write.mode("overwrite").format('com.databricks.spark.csv') \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").save('sparkSolution/outputs', header = 'true')
    
sparkParser(pathFile)
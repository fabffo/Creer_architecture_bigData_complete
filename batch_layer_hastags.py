#! /usr/bin/env python3
import json
from kafka import KafkaConsumer
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode
import re, ast, sys, os
import fastavro
import hdfs
import time
import schedule
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata
from elasticsearch import Elasticsearch

def main(input, output, consumer, sc, spark, es):
    print(input)
    # supprime dans elasticsearch l'heure précédente
    es.indices.delete(index='speed-layer-twitter', ignore=[400, 404])
    print("suppression speed-layer avant commencement")


    heureMax = datetime.now().hour + 1
    minutesMax = (datetime.now().hour * 60 ) + datetime.now().minute + 1
    valeur = {}
    list_hastag = []


    #consumer_timeout_ms=1000
    i = 0
    for message in consumer:
        i = i + 1
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))
        print("OFFSET : ", message.offset)
        # consumer.get.offsett
        # offsset précédent
        # offsset = offsset - 1
        valeur = json.loads(message.value.decode())
        print(valeur)
        timestamp = valeur["timestamp"]
        datehashtag = valeur['datehashtag']
        hastags = valeur['hashtags']

        # liste python sc.parrallize
        list_hastag.append(valeur)

        # On détermine l'heure max
        dateHas = datetime.strptime(datehashtag, '%Y/%m/%d %H:%M:%S')
        heureTopic = dateHas.hour
        minuteTopic = dateHas.minute
        totalMinutesTopic = dateHas.hour * 60 + dateHas.minute
        print("heure: %d, minute: %d" % (heureTopic, minuteTopic))
        print("minutetopic: %d, minutemax: %d" % (totalMinutesTopic, minutesMax))
        # kapa architecture

        # if hr max > je sorts avec offsset -1 et commit offsset

        print(hastags, "à la date : ", datehashtag)
        if totalMinutesTopic >= minutesMax:
            # Mark this message as fully consumed
            # so it can be included in the next commit
            #consumer.task_done(message)

            # Commit the message that was just consumed
            #consumer.commit()
            #offsets = {message.key:OffsetAndMetadata(message.offset, '')}
            meta = consumer.partitions_for_topic("topic_hashtags")
            tp = TopicPartition(message.topic, message.partition)
            offsets = {tp: OffsetAndMetadata(message.offset, None)}
            consumer.commit(offsets=offsets)
            #consumer.commit(OffsetAndMetadata(message.offset, meta))
            # meta = consumer.partitions_for_topic("topic_hashtags")
            # options = {}
            # options[message.partition] = OffsetAndMetadata(message.offset - 1, meta)
            # consumer.commit(options)
            print("On stoppe la boucle")
            break


    # créer dataframe avro
    # sauvegarder hdfs
    schema = {
            "namespace": "ffo.hashtag",
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "datehashtag", "type": "string"},
                {"name": "timestamp", "type": "int"},
                {"name": "hashtags", "type": {"type": "array", "items": "string"}, "default": {}},
            ]
    }

    hdfs_client = hdfs.InsecureClient("http://0.0.0:50070")
    with hdfs_client.write(output, overwrite=True) as avro_file:
        fastavro.writer(avro_file, schema, list_hastag)
        print("ok")


    # exploite dataframe opérations explode group by
    # pousse dans elasticsearch
    distData = sc.parallelize(list_hastag)

    # Convert to a Spark dataframe
    df = distData.toDF()

    # Cache data to avoid re-computing everything
    df.persist()
    dt = df.select(explode(df.hashtags), df.datehashtag, df.timestamp)

    for f in dt.collect():
        print("==============enregistrement elasticsearch===================")
        print(f.col)
        print(f.datehashtag)
        print("==============enregistrement elasticsearch===================")
        elastic(f.timestamp, f.datehashtag, f.col, es)
    #dt.createTempView("datasql")
    #spark.sql("SELECT * FROM datasql").show()
    #print(df)

def elastic(id, date, hashtag, es):
    doc = {
        'date': date,
        'hashtag': hashtag,
    }
    res = es.index(index="batch-layer-twitter", id=id, body=doc)



if __name__ == "__main__":
    # autocommit =
    consumer = KafkaConsumer("topic_hashtags", bootstrap_servers='localhost:9092',
                             group_id="batch_layer_hastags", enable_auto_commit=False, auto_offset_reset='largest',
    auto_commit_interval_ms=60 * 1000,
    consumer_timeout_ms=-1)
    sc = SparkContext()
    spark = SparkSession.builder.getOrCreate()

    es = Elasticsearch()

    # récupération des paramétres
    input = sys.argv[1]
    output = sys.argv[2]

    #on lance le main
    main(input, output, consumer, sc, spark, es)

    print("la suite !!!!!!!!!!!!!!!!!!!!!!!!!!")
    schedule.every(3600).seconds.do(lambda: main(input, output, consumer, sc, spark, es))
    while 1:
        schedule.run_pending()
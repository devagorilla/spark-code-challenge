package com.snowplowanalytics.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import org.json4s.jackson.JsonMethods._

import org.json4s.JsonDSL._


object CSVToJSON {

  private val AppName = "CSV2JSON"

  // Run the word count. Agnostic to Spark's current mode of operation: can be run from tests as well as from main
  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil) {

    val sc = {
      val conf = new SparkConf().setAppName(AppName).setJars(jars)
      for (m <- master) {
        conf.setMaster(m)
      }
      new SparkContext(conf)
    }

    //args(0) -- input file
    val txt = sc.textFile(args(0))

    //args(2) -- save it to local file ?
    val store = if(args(2).equalsIgnoreCase("true")) true else false

    //args(2) -- publish to Kafka ?
    val publish = if(args(3).equalsIgnoreCase("true")) true else false

    def parserRows(content: RDD[String]): RDD[Array[String]] = {
      //add code here to parse the csv rows

      content.map(line => line.split(",").map(_.trim))
    }

    def getHeaders(rdd: RDD[Array[String]]): Array[String] = {
      //add code here to get headers
      rdd.first()
    }

    def getMap(content: RDD[Array[String]], bcHeaders: Broadcast[Array[String]]): RDD[Map[String, String]] = {
      val noHeader = content.mapPartitionsWithIndex(
        (i, iterator) =>
          if (i == 0 && iterator.hasNext) {
            iterator.next
            iterator
          } else iterator)

      val headers = bcHeaders.value

      noHeader.map(splits => headers.zip(splits).toMap)


    }

    def toJsonLines(rdd: RDD[Map[String, String]]): RDD[String] = {
      //add code here to convert the map into json string

      rdd.map { a => compact(render(a)) }
    }

    def writeJsonLinesTxtFile(rdd: RDD[String]): Unit = {
      //add code to save the file here
      rdd.saveAsTextFile(args(1))
    }


    def publish2Kafka(rdd: RDD[String]): Unit = {

      val topicName = "test"

      val strProducer = Producer[String](topicName)

      rdd.take(5).foreach { l =>
        strProducer.send(l)
      }


    }

    val parsed = parserRows(txt).cache()
    val headers = getHeaders(parsed)
    val bcHeaders = sc.broadcast(headers)
    val mappedCsv = getMap(parsed, bcHeaders)
    val jsonLines = toJsonLines(mappedCsv)

    if(store) writeJsonLinesTxtFile(jsonLines)

    if (publish) publish2Kafka(jsonLines)

  }

}


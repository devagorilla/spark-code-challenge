package com.snowplowanalytics.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.json4s.JsonDSL._

//import kafka.producer.ProducerConfig
//import java.util.Properties
//import kafka.producer.Producer
//import scala.util.Random
//import kafka.producer.Producer
//import kafka.producer.Producer
//import kafka.producer.Producer
//import kafka.producer.KeyedMessage
//import java.util.Date


/**
  * Created by benjarman on 9/29/15.
  */
object CSVToJson {

  private val AppName = "CSV2Json"

  // Run the word count. Agnostic to Spark's current mode of operation: can be run from tests as well as from main
  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil) {



    val sc = {
      val conf = new SparkConf().setAppName(AppName).setJars(jars)
      for (m <- master) {
        conf.setMaster(m)
      }
      new SparkContext(conf)
    }


    val txt = sc.textFile(args(0))

    def parserRows(content: RDD[String]): RDD[Array[String]] = {
      //add code here to parse the csv rows

      content.map(line => line.split(",").map(_.trim))
    }

    def getHeaders(rdd: RDD[Array[String]]): Array[String] = {
      //add code here to get headers
      rdd.first()
     //rdd.zipWithIndex().filter(_._2 == 0).map(x => x._1(0))

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
     //val producer = Producer[String]("Cartier_3day_auctions")


      //val strProducer = Producer[String]("Cartier_3day_auctions")

      //strProducer.send(rdd.take(1))
    }

    //10000 colobu localhost:9092
//    def publish2Kafka(rdd: RDD[String]) : Unit = {
//
//      //val events = 1
//      val topic = "code-challenge"
//      val brokers = "54.149.56.104:9092"
//      //val rnd = new Random()
//      val props = new Properties()
//      props.put("metadata.broker.list", brokers)
//      props.put("serializer.class", "kafka.serializer.StringEncoder")
//      //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
//      props.put("producer.type", "async")
//      //props.put("request.required.acks", "1")
//
//      val config = new ProducerConfig(props)
//      val producer = new Producer[String, String](config)
//      //val t = System.currentTimeMillis()
//      //for (nEvents <- Range(0, events)) {
//        //val runtime = new Date().getTime()
//        val ip = "64.135.177.114"
//        rdd.map { x =>
//
//          val data = new KeyedMessage[String, String](topic, ip, x)
//          producer.send(data)
//
//        }
//
//      //}
//
//      //System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t));
//      producer.close()
//
//    }

    val parsed = parserRows(txt).cache()
    val headers = getHeaders(parsed)
    val bcHeaders = sc.broadcast(headers)
    val mappedCsv = getMap(parsed, bcHeaders)
    val jsonLines = toJsonLines(mappedCsv)
    writeJsonLinesTxtFile(jsonLines)
    //publish2Kafka(jsonLines)

  }

}



class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
}

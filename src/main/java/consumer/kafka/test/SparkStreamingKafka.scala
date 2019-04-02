package consumer.kafka.test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

class SparkStreamingKafka {
  def main(args: Array[String]): Unit = {

     val _sparkConf: SparkConf = new SparkConf()
     val sparkContext: SparkContext = new SparkContext(_sparkConf)
//     val javaSparkContext: JavaSparkContext = new JavaSparkContext(_sparkConf)
     val streamingContext: StreamingContext = new StreamingContext(sparkContext, Durations.seconds(30))

     val kafkaParams = Map[String, Object](
       "bootstrap.servers" -> "10.25.16.164:9092,10.25.22.115:9092,10.25.21.72:9092",
       "key.deserializer" -> classOf[StringDeserializer],
       "value.deserializer" -> classOf[StringDeserializer],
       "group.id" -> "use_a_separate_group_id_for_each_stream",
       "auto.offset.reset" -> "latest",
       "enable.auto.commit" -> (false: java.lang.Boolean)
     )

     val topics = Array("bill-test")
     val stream = KafkaUtils.createDirectStream[String, String](
       streamingContext,
       PreferConsistent,
       Subscribe[String, String](topics, kafkaParams)
     )

     val offsetRanges = Array(
       // topic, partition, inclusive starting offset, exclusive ending offset
       OffsetRange("test", 0, 0, 100),
       OffsetRange("test", 1, 0, 100)
     )

//     org.apache.spark.streaming.kafka010.KafkaUtils.createRDD[String, String](sparkContext, kafkaParams, offsetRanges, PreferConsistent)

//     KafkaUtils.createRDD[String, String](javaSparkContext,  kafkaParams, offsetRanges, PreferConsistent);

   }
}

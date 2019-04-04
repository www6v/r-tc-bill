package ucloud.utrc.bill

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{TaskContext, SparkConf, SparkContext}
import org.slf4j.{LoggerFactory, Logger}

object SparkStreamingKafka {
  private val logger: Logger = LoggerFactory.getLogger(classOf[SparkStreamingKafkaForJava])

  val BOOTSTRAP_SERVERS: String = "10.25.16.164:9092,10.25.22.115:9092,10.25.21.72:9092"
  val TOPIC_NAME: String = "urtc_bill_log"
  val GROUP_ID: String = "urtc_bill_group"

  def main(args:Array[String]) : Unit = {

    val _sparkConf: SparkConf = new SparkConf()
    val sparkContext: SparkContext = new SparkContext(_sparkConf)
    val streamingContext: StreamingContext = new StreamingContext(sparkContext, Durations.seconds(30))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> GROUP_ID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(TOPIC_NAME)
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //     val offsetRanges = Array(
    //       // topic, partition, inclusive starting offset, exclusive ending offset
    //       OffsetRange("test", 0, 0, 100),
    //       OffsetRange("test", 1, 0, 100)
    //     )
    //     org.apache.spark.streaming.kafka010.KafkaUtils.createRDD[String, String](sparkContext, kafkaParams, offsetRanges, PreferConsistent)
    //     KafkaUtils.createRDD[String, String](javaSparkContext,  kafkaParams, offsetRanges, PreferConsistent);

    val filtedRdd = stream.filter(consumerRecord => {
      val jsonObject = JSON.parseObject(consumerRecord.value())
      val mstag = jsonObject.getString("mstag")

      logger.info( "mstag: " + mstag)
      logger.info( "mstag.equals(STAT)" + mstag.equals("STAT") )

      mstag.equals("STAT")
    });

    val handlerdRdd = filtedRdd.map( consumerRecord => {
//      val offsetRanges = consumerRecord.asInstanceOf[HasOffsetRanges].offsetRanges

      val jsonObject = JSON.parseObject(consumerRecord.value())

//      var appId = jsonObject.getOrDefault("appId",null)
//      var userId = jsonObject.getOrDefault("userId",null)
//      val roomId = jsonObject.getOrDefault("roomId",null)
//      val streamId = jsonObject.getOrDefault("streamId",null)

      var appId = jsonObject.getString("appId")
      var userId = jsonObject.getString("userId")
      val streamId = jsonObject.getString("streamId")
      val id = appId + "-" + userId;

      logger.info( "appId: " + appId)
      logger.info( "userId: " + userId)
      logger.info( "streamId: " + streamId)
      logger.info( "id: " + id)

//      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      (id,streamId)
    });

    val rddRow = handlerdRdd.mapValues(streamId => 1);
    val rddAgg = rddRow.reduceByKey(_ + _);

    rddAgg.foreachRDD { rdd =>

      rdd.foreachPartition { pair=> {
            val itor = pair.toIterator
            while(itor.hasNext) {
              val row = itor.next();
              val roomId = row._1;
              val count = row._2;

              logger.info( "roomId: " + roomId)
              logger.info( "count: " + count)

              DbStore.insertDB(roomId, count)
            }
          }
      }

    }


//    stream.foreachRDD { rdd =>
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      rdd.foreachPartition(consumerRecords => {
//        if (consumerRecords.hasNext) {
//          val next: ConsumerRecord[String, String] = consumerRecords.next
//          val key: String = next.key
//          val value: String = next.value
//          if (value != null) {
//            //            logger.info("value:" + value)
//          }
//          DbStore.insertDB(value, 345)
//        }
//      })
//
//      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//    }

    try {
      streamingContext.start
      streamingContext.awaitTermination
    }
    catch {
      case ex: Exception => {
        //      streamingContext.ssc.sc.cancelAllJobs
        streamingContext.stop(true, false)
        System.exit(-1)
      }
    }
  }
}

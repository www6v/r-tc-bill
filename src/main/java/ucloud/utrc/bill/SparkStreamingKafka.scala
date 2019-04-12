package ucloud.utrc.bill

import com.alibaba.fastjson.{JSONException, JSON}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{TaskContext, SparkConf, SparkContext}
import org.slf4j.{LoggerFactory, Logger}
import ucloud.utrc.bill.mybatis.RtcBillDataAccess


object SparkStreamingKafka {
  private val logger: Logger = LoggerFactory.getLogger(SparkStreamingKafka.getClass)

  val BOOTSTRAP_SERVERS: String = "10.25.16.164:9092,10.25.22.115:9092,10.25.21.72:9092"
  val TOPIC_NAME: String = "urtc_bill_log"
  val GROUP_ID: String = "urtc_bill_group"
  val DURATIONS_TIME: Long = 30 * 5;
  val SEPERATOR : String =  "|";

  def main(args:Array[String]) : Unit = {

    val _sparkConf: SparkConf = new SparkConf()
    val sparkContext: SparkContext = new SparkContext(_sparkConf)
    val streamingContext: StreamingContext = new StreamingContext(sparkContext, Durations.seconds(DURATIONS_TIME))

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

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val filtedRdd = stream
      .transform(rdd => {
         offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
         rdd
    })
      .filter(consumerRecord => StringUtils.isNotEmpty(consumerRecord.value()))
      .filter(filterFunc = consumerRecord => {
        var result = false;
        try {
          val jsonObject = JSON.parseObject(consumerRecord.value())
          val mstag = jsonObject.getString("mstag")
          val _type = jsonObject.getString("type")

          logger.info("mstag: " + mstag + " type:" + _type)

          val isBillLog: Boolean = mstag.equals("STAT")
          if(!StringUtils.isNotEmpty(_type)) {
            result = false
          } else {
            val isPullStreaming: Boolean = _type.equals("PULL")
            result = isBillLog && isPullStreaming
          }
          result
        } catch {
          case ex: JSONException => {
            result
          }
        }
      });


//    val duplicatedRdd = filtedRdd.map( consumerRecord => {
//      val consumerRecordLine = consumerRecord.value();
//      val jsonObject = JSON.parseObject(consumerRecordLine)
//
//      val streamId = jsonObject.getString("streamId")
//
//      (consumerRecordLine, streamId)
//    })

    val handlerdRdd = filtedRdd.map( consumerRecord => {
//      val offsetRanges = consumerRecord.asInstanceOf[HasOffsetRanges].offsetRanges

      val jsonObject = JSON.parseObject(consumerRecord.value())

//      var appId = jsonObject.getOrDefault("appId",null)
//      var userId = jsonObject.getOrDefault("userId",null)
//      val roomId = jsonObject.getOrDefault("roomId",null)
//      val streamId = jsonObject.getOrDefault("streamId",null)

      val appId = jsonObject.getString("appId")
      val userId = jsonObject.getString("userId")
      val roomId = jsonObject.getString("roomId")
      val profile = jsonObject.getString("profile")

      val streamId = jsonObject.getString("streamId")

      val time = jsonObject.getLong("time")

//      val id = appId + SEPERATOR + userId + SEPERATOR + roomId + SEPERATOR + profile;
      val id = appId + SEPERATOR + userId + SEPERATOR + roomId + SEPERATOR + streamId + SEPERATOR + profile;

      logger.info( "streamId: " + streamId)
      logger.info( "id: " + id)

//    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

//      (id,streamId)
      (id,(1, time,time))
    });

//    val rddRow = handlerdRdd.mapValues(streamId => 1);
//    val rddAgg = rddRow.reduceByKey(_ + _);

    val rddAgg = handlerdRdd.reduceByKey((x1, x2) =>(x1._1 + x2._1,
      { if(  x1._2 > x2._2 )
         x1._2
        else
         x2._2 },
       { if(  x1._3 > x2._3 )
         x2._3
       else
         x1._3 }
      ))

    rddAgg.foreachRDD { rdd =>

      rdd.foreachPartition { pair=> {
            val itor = pair.toIterator
            while(itor.hasNext) {
              val row = itor.next();
              val roomId = row._1;
              val count = row._2._1;
              val endTime = row._2._2;
              val startTime = row._2._3;

              val appIdIndex = roomId.indexOf(SEPERATOR);
              val appId = roomId.substring(0, appIdIndex)

              val profileIndex = roomId.lastIndexOf(SEPERATOR) + 1;
              val length = roomId.length;
              val profile = roomId.substring(profileIndex, length)

              logger.info( "roomId: " + roomId)
              logger.info( "count: " + count)
              logger.info( "appId: " + appId)
              logger.info( "profile: " + profile)

//              DbStore.insertDB(roomId, count)
              RtcBillDataAccess.insertDB(appId, roomId, count, profile, endTime, startTime)
            }
          }
      }

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
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

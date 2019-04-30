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
import org.joda.time.DateTime
import org.slf4j.{LoggerFactory, Logger}

import ucloud.utrc.bill.mybatis.RtcBillDataAccess

object SparkStreamingKafka {
  private val logger: Logger = LoggerFactory.getLogger(SparkStreamingKafka.getClass)

  val BOOTSTRAP_SERVERS: String =  PropertiesUtil.getPropString("kafka.bootstrap.servers")
  val TOPIC_NAME: String = PropertiesUtil.getPropString("kafka.topic")
  val GROUP_ID: String =  PropertiesUtil.getPropString("kafka.group.id")
  val DURATIONS_TIME: Long = 30 * 3;
  val SEPERATOR : String =  "|";
  val CHECKPOINT_DIR = "rtc_bill_checkpoint"

  def functionToCreateContext(): StreamingContext = {
    val _sparkConf: SparkConf = new SparkConf()
    val sparkContext: SparkContext = new SparkContext(_sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Durations.seconds(DURATIONS_TIME))

    ssc.checkpoint(CHECKPOINT_DIR)   // set checkpoint directory
    ssc
  }

  def main(args:Array[String]) : Unit = {
    val streamingContext = StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext _)

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

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val filtedRdd = stream
      .transform(rdd => {
         offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
         rdd
    })
      .filter(consumerRecord => StringUtils.isNotEmpty(consumerRecord.value()) && consumerRecord.value().contains("mstag"))
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

    val handlerdRdd = filtedRdd.map( consumerRecord => {
      val jsonObject = JSON.parseObject(consumerRecord.value())

      val appId = jsonObject.getString("appId")
      val userId = jsonObject.getString("userId")
      val roomId = jsonObject.getString("roomId")
      val profile = jsonObject.getString("profile")
      val streamId = jsonObject.getString("streamId")

      val video = jsonObject.getInteger("video")
      val audio = jsonObject.getInteger("audio")

      val ts = jsonObject.getString("ts")
      var time = 111L;
      if(ts ==null) {
        time = 111L;
      }
      else {
        val dt = DateTime.parse(ts);
        time = dt.getMillis();
      }

      val id = appId + SEPERATOR + userId + SEPERATOR + roomId + SEPERATOR + streamId + SEPERATOR + profile;

      logger.info( "streamId: " + streamId)
      logger.info( "id: " + id)

      (id,(1, time, time, video, audio))
    });

//    val rddRow = handlerdRdd.mapValues(streamId => 1);
//    val rddAgg = rddRow.reduceByKey(_ + _);

    val rddAgg = handlerdRdd.reduceByKey((x1, x2) =>(
      x1._1 + x2._1,
      { if(  x1._2 > x2._2 ) 
         x1._2
        else
         x2._2 },
       { if(  x1._3 > x2._3 )
         x2._3
       else
         x1._3 },
      x1._4 + x2._4,
      x1._5 + x2._5
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

              val videoCount = row._2._4;
              val audioCount = row._2._5

              val appIdIndex = roomId.indexOf(SEPERATOR);
              val appId = roomId.substring(0, appIdIndex)

              val profileIndex = roomId.lastIndexOf(SEPERATOR) + 1;
              val length = roomId.length;
              val profile = roomId.substring(profileIndex, length)

              logger.info( "roomId: " + roomId)
              logger.info( "count: " + count)
              logger.info( "appId: " + appId)
              logger.info( "profile: " + profile)

              RtcBillDataAccess.insertDB(appId, roomId, count, profile, endTime, startTime, videoCount, audioCount)
            }
          }
      }

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

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

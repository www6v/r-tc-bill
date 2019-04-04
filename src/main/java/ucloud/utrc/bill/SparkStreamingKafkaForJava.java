package ucloud.utrc.bill;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkStreamingKafkaForJava {
    private  final static Logger logger = LoggerFactory.getLogger(SparkStreamingKafkaForJava.class);

    public static final String BOOTSTRAP_SERVERS = "10.25.16.164:9092,10.25.22.115:9092,10.25.21.72:9092";
    public static final String TOPIC_NAME = "urtc_bill_log";
    public static final String GROUP_ID ="urtc_bill_group";

    public static void main(String args[]) {

         SparkConf _sparkConf= new SparkConf();
         JavaSparkContext sparkContext= new JavaSparkContext(_sparkConf);
         JavaStreamingContext streamingContext= new JavaStreamingContext(sparkContext, Durations.seconds(30));
//         SQLContext sqlContext = new SQLContext(sparkContext);

         Map<String, Object> kafkaParams = new HashMap<>();
         kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVERS);
         kafkaParams.put("key.deserializer", StringDeserializer.class);
         kafkaParams.put("value.deserializer", StringDeserializer.class);
         kafkaParams.put("group.id", GROUP_ID);
         kafkaParams.put("auto.offset.reset", "latest");
         kafkaParams.put("enable.auto.commit", false);

         Collection<String> topics = Arrays.asList(TOPIC_NAME);

         JavaInputDStream<ConsumerRecord<String, String>> stream =
                 KafkaUtils.createDirectStream(
                         streamingContext,
                         LocationStrategies.PreferConsistent(),
                         ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                 );

         /////
//         stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
//         OffsetRange[] offsetRanges1 = {
//                 // topic, partition, inclusive starting offset, exclusive ending offset
//                 OffsetRange.create(TOPIC_NAME, 0, 0, 100),
//                 OffsetRange.create(TOPIC_NAME, 1, 0, 100)
//         };
//         JavaRDD<ConsumerRecord<String, String>> rdd1 = KafkaUtils.createRDD(
//                 sparkContext,
//                 kafkaParams,
//                 offsetRanges1,
//                 LocationStrategies.PreferConsistent()
//         );
         /////

         stream.foreachRDD(rdd -> {
             OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
             rdd.foreachPartition(consumerRecords -> {

                 OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                 System.out.println( "o.topic(): " + o.topic()
                         + "o.partition(): " + o.partition()
                         + "o.fromOffset() " + o.fromOffset()
                         + "o.untilOffset() " + o.untilOffset());

                 if(consumerRecords.hasNext()){
                     ConsumerRecord<String, String> next = consumerRecords.next();
                     String key = next.key();
                     String value = next.value();

                     if(value!=null){
                         logger.info("value:" + value);
                     }
                     DbStore.insertDB(value, 345);
                 }
             });

             // some time later, after outputs have completed
             ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
         });

//         stream.foreachRDD ( rdd -> {
//                     rdd.foreachPartition(partitionOfRecords -> {
////                 // ConnectionPool is a static, lazily initialized pool of connections
////                 Connection connection = ConnectionPool.getConnection();
////                 partitionOfRecords.foreach(record -> connection.send(record));
////                 ConnectionPool.returnConnection(connection);  // return to the pool for future reuse
//                     });
//                 });

//         stream.foreachRDD(rdd -> {
//             OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//
//             // some time later, after outputs have completed
//             ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
//         });

         try {
             streamingContext.start();
             streamingContext.awaitTermination();
         }catch (Exception ex ) {
             streamingContext.ssc().sc().cancelAllJobs();
             streamingContext.stop(true, false);
             System.exit(-1);
         }
     }
}

package ucloud.utrc.bill;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import consumer.kafka.client.DataAccess;

public class SparkStreamingKafkaForJava {

    private  final static Logger logger = LoggerFactory.getLogger(SparkStreamingKafkaForJava.class);

     public static void main(String args[]) {

         DataAccess dataAccess = new DataAccess();

         SparkConf _sparkConf= new SparkConf();
//         val sparkContext: SparkContext = new SparkContext(_sparkConf)
         JavaSparkContext sparkContext= new JavaSparkContext(_sparkConf);
         JavaStreamingContext streamingContext= new JavaStreamingContext(sparkContext, Durations.seconds(30));
//         SQLContext sqlContext = new SQLContext(sparkContext);

         Map<String, Object> kafkaParams = new HashMap<>();
         kafkaParams.put("bootstrap.servers", "10.25.16.164:9092,10.25.22.115:9092,10.25.21.72:9092");
         kafkaParams.put("key.deserializer", StringDeserializer.class);
         kafkaParams.put("value.deserializer", StringDeserializer.class);
         kafkaParams.put("group.id", "group1");
         kafkaParams.put("auto.offset.reset", "latest");
         kafkaParams.put("enable.auto.commit", false);

         Collection<String> topics = Arrays.asList("bill-test");

         JavaInputDStream<ConsumerRecord<String, String>> stream =
                 KafkaUtils.createDirectStream(
                         streamingContext,
                         LocationStrategies.PreferConsistent(),
                         ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                 );

         stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

         OffsetRange[] offsetRanges1 = {
                 // topic, partition, inclusive starting offset, exclusive ending offset
                 OffsetRange.create("bill-test", 0, 0, 100),
                 OffsetRange.create("bill-test", 1, 0, 100)
         };

         JavaRDD<ConsumerRecord<String, String>> rdd1 = KafkaUtils.createRDD(
                 sparkContext,
                 kafkaParams,
                 offsetRanges1,
                 LocationStrategies.PreferConsistent()
         );

         stream.foreachRDD(rdd -> {
//             SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
//             List<Row> rowList1 = new ArrayList<>();
//             dataAccess.insertdb( rowList1, "abc", 109,sqlContext);

//             List<ConsumerRecord<String, String>> collect = rdd.collect();
//             Iterator<ConsumerRecord<String, String>> iterator = collect.iterator();
//             while( iterator.hasNext()) {
//                 ConsumerRecord<String, String> next = iterator.next();
//                 String key = next.key();
//                 String value = next.value();
//                 dataAccess.insertdb( rowList1, key, 509,sqlContext);
//             }

             rdd.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
                 @Override
                 public void call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
//                     dataAccess.insertdb( rowList1, stringStringConsumerRecord.key(), 509,sqlContext);
                 }
             });

             OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
             rdd.foreachPartition(consumerRecords -> {
//                 SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());// 不能在这里

                 OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                 System.out.println( "o.topic(): " + o.topic()
                         + "o.partition(): " + o.partition()
                         + "o.fromOffset() " + o.fromOffset()
                         + "o.untilOffset() " + o.untilOffset());

//                 List<Row> rowList1 = new ArrayList<>();
//                 dataAccess.insertdb( rowList1, "ttt", 111 ,sqlContext );

                 if(consumerRecords.hasNext()){
                     ConsumerRecord<String, String> next = consumerRecords.next();
                     String key = next.key();
                     String value = next.value();

                     if(key!=null){
                         System.out.println("key:" + key);
                         logger.info("key:" + key);
                     }
                     if(value!=null){
                         System.out.println("value:" + value);
                         logger.info("value:" + value);
                     }
                     Temp.insertDB(key, 345);
//                     List<Row> rowList1 = new ArrayList<>();
//                     dataAccess.insertdb( rowList1, key, 111 ,sqlContext );
                 }
             });
         });



         stream.foreachRDD ( rdd -> {
                     rdd.foreachPartition(partitionOfRecords -> {
//                 // ConnectionPool is a static, lazily initialized pool of connections
//                 Connection connection = ConnectionPool.getConnection();
//                 partitionOfRecords.foreach(record -> connection.send(record));
//                 ConnectionPool.returnConnection(connection);  // return to the pool for future reuse

                         partitionOfRecords.hasNext();
                     });
                 });


         stream.foreachRDD(rdd -> {
             OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

             // some time later, after outputs have completed
             ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
         });


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

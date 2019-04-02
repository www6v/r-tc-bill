/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consumer.kafka.client;

import java.io.Serializable;
import java.util.*;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RtcBill implements Serializable {
    private  final Logger logger = LoggerFactory.getLogger(RtcBill.class);

  public void start() throws InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    run();
  }

  private void run() {
    logger.info("in RtcBill run()");

    Properties props = new Properties();
    props.put("zookeeper.hosts", "10.25.16.164,10.25.22.115,10.25.21.72");
    props.put("zookeeper.port", "2181");
    props.put("kafka.topic", "bill-test");
    props.put("kafka.consumer.id", "bill-kafka-consumer");

    // Optional Properties
//  props.put("consumer.forcefromstart", "true");
    props.put("consumer.forcefromstart", "false");
//  props.put("max.poll.records", "100");
    props.put("max.poll.records", "5");
    props.put("consumer.fillfreqms", "1000");
//  props.put("consumer.backpressure.enabled", "true");
    props.put("consumer.backpressure.enabled", "false");

    props.put("consumer.queue.to.throttle", "10000"); /// add

    //Kafka properties
    props.put("bootstrap.servers", "10.25.16.164:9092,10.25.22.115:9092,10.25.21.72:9092");
//    props.put("security.protocol", "SSL");
//    props.put("ssl.truststore.location","~/kafka-securitykafka.server.truststore.jks");
//    props.put("ssl.truststore.password", "test1234");

    SparkConf _sparkConf = new SparkConf();
    JavaSparkContext sparkContext = new JavaSparkContext(_sparkConf);
    JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.seconds(30));
    SQLContext sqlContext = new SQLContext(sparkContext);

    int numberOfReceivers = 1;

    JavaDStream<MessageAndMetadata<byte[]>> unionStreams = ReceiverLauncher.launch(
        jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
//    JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
//        .getPartitionOffset(unionStreams, props);

      DataAccess dataAccess = new DataAccess();
//      List<Row> rowList1 = new ArrayList<>();
//      dataAccess.insertdb(rowList1,"123",567,sqlContext);

    unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata<byte[]>>>() {
      @Override
      public void call(JavaRDD<MessageAndMetadata<byte[]>> rdd) throws Exception {
//          List<MessageAndMetadata<byte[]>> collect = rdd.collect();
//          RDD<MessageAndMetadata<byte[]>> rdd1 = rdd.rdd();
//          new DataAccess( ).insert1(rdd.rdd(),_sparkConf);
//          new DataAccess( ).insert1(rdd.rdd(),JavaSparkContext.toSparkContext(sparkContext));
            List<Row> rowList1 = new ArrayList<>();
            dataAccess.insertdb(rowList1,"123",321,sqlContext);

//          MessageAndMetadata<byte[]>[] collectArray = (MessageAndMetadata<byte[]>[]) rdd1.collect();
//          int length = collectArray.length;
//          new DataAccess( ).insertdb(rowList1, String.valueOf(length), String.valueOf(length), sqlContext); // verify
//
//          for( int i =0; i<length; i++) {
//              MessageAndMetadata<byte[]> messageAndMetadata = collectArray[i];
//              byte[] key = messageAndMetadata.getKey();
//              byte[] payload = messageAndMetadata.getPayload();
//              if(key !=null) {
//                  String s = new String(key);
//                  new DataAccess( ).insertdb(rowList1, s, "out1-value", sqlContext);
//              }
//              if(payload !=null) {
//                  String s1 = new String(payload);
//                  new DataAccess( ).insertdb(rowList1, "out1-key1", s1, sqlContext);
//              }
//
//              if(key !=null && payload !=null) {
//                  String s = new String(key);
//                  String s1 = new String(payload);
//                  new DataAccess( ).insertdb(rowList1, s, s1, sqlContext);
//              }
//          }

//          scala.collection.Iterator<MessageAndMetadata<byte[]>> iterator = rdd1.iterator();
//          MessageAndMetadata<byte[]>[] take = rdd1.take(1);



//          rdd.foreachPartition(new VoidFunction<Iterator<MessageAndMetadata<byte[]>>>() {
//
//			@Override
//			public void call(Iterator<MessageAndMetadata<byte[]>> mmItr) throws Exception {
//
//                List<Row> rowList = new ArrayList<>();
//
//				while(mmItr.hasNext()) {
//					MessageAndMetadata<byte[]> mm = mmItr.next();
//					byte[] key = mm.getKey();
//					byte[] value = mm.getPayload();
//					Headers headers = mm.getHeaders();
//					if(key != null) {
//                        logger.info(" key :" + new String(key));
//                        new DataAccess().insertdb(rowList, new String(key), 789, sqlContext);
//                    }
//					if(value != null) {
//                        logger.info(" Value :" + new String(value));
//                        new DataAccess().insertdb(rowList, "defautlKey", 78, sqlContext);
//                    }
//                    ///
//                    if(key != null&& value != null) {
//
//                        logger.info(" key :" + new String(key));
//                        logger.info(" Value :" + new String(value));
//
////                        new DataAccess( ).insert1(new String(key),new String(value),JavaSparkContext.toSparkContext(sparkContext));
////                        new DataAccess( ).insert1(rowList,new String(key),new String(value),JavaSparkContext.toSparkContext(sparkContext));
//                        new DataAccess( ).insertdb(rowList,new String(key),7,sqlContext);
//                    }
//                    ///
//					if(headers != null) {
//						Header[] harry = headers.toArray();
//						for(Header header : harry) {
//							String hkey = header.key();
//							byte[] hvalue = header.value();
//							if(hvalue != null && hkey != null)
////								System.out.println("Header Key :" + hkey + " Header Value :" + new String(hvalue));
//                                logger.info("Header Key :" + hkey + " Header Value :" + new String(hvalue));
//						}
//					}
//				}
//			}
//		});
      }
    });

    //Persists the Max Offset of given Kafka Partition to ZK
//    ProcessedOffsetManager.persists(partitonOffset, props);

    try {
      jsc.start();
      jsc.awaitTermination();
    }catch (Exception ex ) {
      jsc.ssc().sc().cancelAllJobs();
      jsc.stop(true, false);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    RtcBill consumer = new RtcBill();
    consumer.start();
  }
}

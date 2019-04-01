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

  @SuppressWarnings("deprecation")
  private void run() {
    logger.info("in RtcBill run()");

    Properties props = new Properties();
    props.put("zookeeper.hosts", "10.25.16.164,10.25.22.115,10.25.21.72");
    props.put("zookeeper.port", "2181");
    props.put("kafka.topic", "bill-test");
    props.put("kafka.consumer.id", "bill-kafka-consumer");
    // Optional Properties
    // Optional Properties
    props.put("consumer.forcefromstart", "true");
//    props.put("max.poll.records", "100");
      props.put("max.poll.records", "1");
    props.put("consumer.fillfreqms", "1000");
//    props.put("consumer.backpressure.enabled", "true");
      props.put("consumer.backpressure.enabled", "false");

    //Kafka properties
    props.put("bootstrap.servers", "10.25.16.164:9092,10.25.22.115:9092,10.25.21.72:9092");
//    props.put("security.protocol", "SSL");
//    props.put("ssl.truststore.location","~/kafka-securitykafka.server.truststore.jks");
//    props.put("ssl.truststore.password", "test1234");

    SparkConf _sparkConf = new SparkConf();
      JavaSparkContext sparkContext = new JavaSparkContext(_sparkConf);
//      JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(30));
      JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.seconds(30));
      SQLContext sqlContext = new SQLContext(sparkContext);
    // Specify number of Receivers you need.
    int numberOfReceivers = 1;

    JavaDStream<MessageAndMetadata<byte[]>> unionStreams = ReceiverLauncher.launch(
        jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
    JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
        .getPartitionOffset(unionStreams, props);

    //Start Application Logic
    unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata<byte[]>>>() {
      @Override
      public void call(JavaRDD<MessageAndMetadata<byte[]>> rdd) throws Exception {
//        new DataAccess( ).insert1(rdd.rdd(),_sparkConf);
//          new DataAccess( ).insert1(rdd.rdd(),JavaSparkContext.toSparkContext(sparkContext));
          List<Row> rowList1 = new ArrayList<>();
          new DataAccess( ).insert1(rowList1,"123","321",sqlContext);

    	rdd.foreachPartition(new VoidFunction<Iterator<MessageAndMetadata<byte[]>>>() {
			
			@Override
			public void call(Iterator<MessageAndMetadata<byte[]>> mmItr) throws Exception {
				while(mmItr.hasNext()) {
					MessageAndMetadata<byte[]> mm = mmItr.next();
					byte[] key = mm.getKey();
					byte[] value = mm.getPayload();
					Headers headers = mm.getHeaders();
					if(key != null)
//						System.out.println(" key :" + new String(key));
                        logger.info(" key :" + new String(key));
					if(value != null)
//						System.out.println(" Value :" + new String(value));
                        logger.info(" Value :" + new String(value));

                    ///
                    if(key != null&& value != null) {

                        logger.info(" key :" + new String(key));
                        logger.info(" Value :" + new String(value));

                        List<Row> rowList = new ArrayList<>();

//                        new DataAccess( ).insert1(new String(key),new String(value),JavaSparkContext.toSparkContext(sparkContext));
//                        new DataAccess( ).insert1(rowList,new String(key),new String(value),JavaSparkContext.toSparkContext(sparkContext));
                        new DataAccess( ).insert1(rowList,new String(key),new String(value),sqlContext);
                    }
                    ///
					if(headers != null) {
						Header[] harry = headers.toArray();
						for(Header header : harry) {
							String hkey = header.key();
							byte[] hvalue = header.value();
							if(hvalue != null && hkey != null)
//								System.out.println("Header Key :" + hkey + " Header Value :" + new String(hvalue));
                                logger.info("Header Key :" + hkey + " Header Value :" + new String(hvalue));
						}
					}
					
				}
				
			}
		});
      }
    });
    //End Application Logic

    //Persists the Max Offset of given Kafka Partition to ZK
    ProcessedOffsetManager.persists(partitonOffset, props);

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

//  public static void writeToDB(String[] args) {
//        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));
//        SQLContext sqlContext = new SQLContext(sparkContext);
//
//      //Arrays.asList("1 tom 5","2 jack 6","3 alex 7");
//      List strings = new ArrayList<>();

//        JavaRDD<String> personData = sparkContext.parallelize(strings);

//        String url = "jdbc:mysql://localhost:3306/test";
//        Properties connectionProperties = new Properties();
//        connectionProperties.put("user","root");
//        connectionProperties.put("password","123456");
//        connectionProperties.put("driver","com.mysql.jdbc.Driver");


//      JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
//          public Row call(String line) throws Exception {
//              String[] splited = line.split(" ");
//              return RowFactory.create(Integer.valueOf(splited[0]),
//                      splited[1],
//                      Integer.valueOf(splited[2]));
//          }
//      });
//
//
//
//

//        List structFields = new ArrayList();
//        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
//        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
//

//        StructType structType = DataTypes.createStructType(structFields);
//

//        DataFrame personsDF = sqlContext.createDataFrame(personsRDD,structType);
//

//        personsDF.write().mode("append").jdbc(url,"person",connectionProperties);
//

//        sparkContext.stop();
//    }
}

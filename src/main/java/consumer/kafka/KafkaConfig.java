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

package consumer.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("serial")
public class KafkaConfig implements Serializable {

  //Default poll size 500.
  public int _pollRecords = 500;
  //Default fill frequency 1 Seconds
  public int _fillFreqMs = 1000;
  //Default minimum fetch size 50
  public int _minpollRecords = 50;


  //Automatic refresh of ZK Coordinator to check for Leader Re-balance
  public int _refreshFreqSecs = 300;
  public int _socketTimeoutMs = 10000;
  //If set to true, it will start from Earliest Offset.
  //Note this is only for first time start of the consumer. 
  //During next successive restart it will either consumes 
  //from Consumed or Processed offset whichever is applicable
  public boolean _forceFromStart = false;
  //PID Controller based back-pressure mechanism to rate control
  public boolean _backpressureEnabled = true;
  public int _maxRestartAttempts = -1;
  public long _stateUpdateIntervalMs = 2000;
  public Map<String,String> _stateConf;
  //Number of fetch consumer will buffer before writing to Spark Block Manager
  public int _numFetchToBuffer = 1;
  //Consumer will throttle to Zero rate if Queued batches reach this value
  //This is to avoid memory pressure
  public int _batchQueueToThrottle = 3;

  //Default PID values for Controller
  public double _proportional = 1.0;
  public double _integral = 0.0;
  public double _derivative = 0;
  
  public String brokerZkPath = "/brokers";
  public String consumerZkPath = "/consumers";

  public Properties props;

  public KafkaConfig(Properties props) {

    this.props = props;
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "none");
    props.put("group.id", props.getProperty("kafka.consumer.id"));


    //ZK Host and Port for Kafka Cluster
    String zkHost = props.getProperty("zookeeper.hosts");
    String zkPort = props.getProperty("zookeeper.port");
    //Kafka Topic
    String kafkaTopic = props.getProperty("kafka.topic");
    //ZK host:port details for Offset writing
    String consumerConnection = "";
    if(props.getProperty("zookeeper.consumer.connection") != null) {
      consumerConnection = props.getProperty("zookeeper.consumer.connection");
    } else {
      String[] zkh = zkHost.split(",");
      for(String host: zkh) {
        String hostport = host + ":" + zkPort;
        consumerConnection = consumerConnection + "," + hostport;
      }
      consumerConnection = consumerConnection.substring(consumerConnection.indexOf(',')+1);
    }
    String consumerId = props.getProperty("kafka.consumer.id");

    if (props.getProperty("zookeeper.broker.path") != null) {
      brokerZkPath = props.getProperty("zookeeper.broker.path");
    }

    if (props.getProperty("zookeeper.consumer.path") != null) {
      consumerZkPath = props.getProperty("zookeeper.consumer.path");
    }

    if (props.getProperty("consumer.forcefromstart") != null) {
      _forceFromStart = Boolean.parseBoolean(props.getProperty("consumer.forcefromstart"));
    }

    if (props.getProperty("consumer.num_fetch_to_buffer") != null) {
      _numFetchToBuffer = Integer.parseInt(props.getProperty("consumer.num_fetch_to_buffer"));
    }

    if (props.getProperty("max.poll.records") != null) {
      _pollRecords = Integer.parseInt(props.getProperty("max.poll.records"));
    }

    if (props.getProperty("min.poll.records") != null) {
      _minpollRecords = Integer.parseInt(props.getProperty("min.poll.records"));
    }

    if (props.getProperty("consumer.fillfreqms") != null) {
      _fillFreqMs = Integer.parseInt(props.getProperty("consumer.fillfreqms"));
    }

    if (props.getProperty("consumer.refresh_freq_sec") != null){
      _refreshFreqSecs = Integer.parseInt(props.getProperty("consumer.refresh_freq_sec"));
    }

    if (props.getProperty("consumer.backpressure.enabled") != null) {
      _backpressureEnabled = Boolean.parseBoolean(props.getProperty("consumer.backpressure.enabled"));
    }

    if (props.getProperty("consumer.backpressure.proportional") != null) {
      _proportional = Double.parseDouble(props.getProperty("consumer.backpressure.proportional"));
    }

    if (props.getProperty("consumer.backpressure.integral") != null) {
      _integral = Double.parseDouble(props.getProperty("consumer.backpressure.integral"));
    }

    if (props.getProperty("consumer.backpressure.derivative") != null) {
      _derivative = Double.parseDouble(props.getProperty("consumer.backpressure.derivative"));
    }

    if (props.getProperty("kafka.consumer.restart.attempt") != null) {
      _maxRestartAttempts = Integer.parseInt(props.getProperty("kafka.consumer.restart.attempt"));
    }

    if (props.getProperty("consumer.queue.to.throttle") != null){
      _batchQueueToThrottle = Integer.parseInt(props.getProperty("consumer.queue.to.throttle"));
    }

    _stateConf = new HashMap<String, String>();
    _stateConf.put(Config.ZOOKEEPER_HOSTS, zkHost);
    _stateConf.put(Config.ZOOKEEPER_PORT, zkPort);
    _stateConf.put(Config.KAFKA_TOPIC, kafkaTopic);
    _stateConf.put(Config.ZOOKEEPER_BROKER_PATH, brokerZkPath);

    _stateConf.put(Config.ZOOKEEPER_CONSUMER_PATH, consumerZkPath);
    _stateConf.put(Config.ZOOKEEPER_CONSUMER_CONNECTION, consumerConnection);
    _stateConf.put(Config.KAFKA_CONSUMER_ID, consumerId);
  }

  public Properties getProperties() {
    return this.props;
  }
}

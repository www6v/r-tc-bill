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

import org.apache.kafka.common.header.Headers;

@SuppressWarnings("serial")
public abstract class KafkaMessageHandler<E> implements Cloneable, Serializable {

    public void init() {
        // do nothing for default implementation
    }

    public MessageAndMetadata<E> handle(long offset, Partition partition, String topic, String consumer, byte[] payload, Headers headers) throws Exception {
        E msg = process(payload);
        MessageAndMetadata<E> m = new MessageAndMetadata<>();
        m.setConsumer(consumer);
        m.setOffset(offset);
        m.setPartition(partition);
        m.setPayload(msg);
        m.setTopic(topic);
        m.setHeaders(headers);
        return m;
    }

    protected abstract E process(byte[] payload);

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (KafkaMessageHandler) super.clone();
    }
}

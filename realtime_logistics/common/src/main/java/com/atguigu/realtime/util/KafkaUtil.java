package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KafkaUtil {


    /**
     * 获取 KafkaSource
     * @param topic 读取的 kafka 主题
     * @param groupId 消费者组
     * @return KafkaSource
     */
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {

        return KafkaSource
                .<String>builder()
                .setBootstrapServers(PropertyUtil.getStringValue("KAFKA_BROKERS"))
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setDeserializer(new KafkaRecordDeserializationSchema<String>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws IOException {
                        long offset = record.offset();
                        byte[] value = record.value();
                        if(value == null) return;
                        String s = new String(value, StandardCharsets.UTF_8);
                        JSONObject jsonObject = JSON.parseObject(s);
                        jsonObject.put("offset", offset);
                        out.collect(jsonObject.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                .build();
    }

    /**
     * 获取 KafkaSink
     * @param topic 要写入的 主题
     * @return KafkaSink
     */
    public static KafkaSink<String> getKafkaSink(String topic){
        return KafkaSink
                .<String>builder()
                .setBootstrapServers(PropertyUtil.getStringValue("KAFKA_BROKERS"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .setTopic(topic)
                                .build()
                )
                //必须是EOS
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //必须设置事务id前缀
                .setTransactionalIdPrefix("atguigu-"+topic)
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1000")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10 * 60 * 1000+"")
                .build();
    }
}

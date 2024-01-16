package com.atguigu.realtime.util;

public class SqlUtil {

    /**
     * 获取 kafkaSource 的配置sql语句
     * @param topic 要读取的 kafka 主题
     * @param groupId 消费者组id
     * @return 返回 sql 语句 : with( config of kafka connector... )
     */
    public static String getKafkaSourceConfigSql(String topic, String groupId) {
        String sql = " WITH ( " +
                " 'connector' = 'kafka', "+
                " 'topic' = '%s', "+
                " 'properties.bootstrap.servers' = '%s', "+
                " 'properties.group.id' = '%s', "+
                " 'scan.startup.mode' = 'earliest-offset', "+
                " 'json.ignore-parse-errors' = 'true', "+
                " 'format' = 'json' "+
                " ) ";
        return String.format(sql, topic, PropertyUtil.getStringValue("KAFKA_BROKERS"), groupId);
    }

    /**
     * 获取 kafkaSink 的配置sql语句
     * @param topic 要写入的 kafka 主题
     * @return 返回 sql 语句 : with( config of kafka connector... )
     */
    public static String getKafkaSinkConfigSql(String topic) {
        String sql = " WITH ( " +
                " 'connector' = 'kafka', "+
                " 'topic' = '%s', "+
                " 'properties.bootstrap.servers' = '%s', "+
                " 'format' = 'json' "+
                " ) ";
        return String.format(sql, topic, PropertyUtil.getStringValue("KAFKA_BROKERS"));
    }

    /**
     * 获取 UpsertKafkaSink 的配置sql语句
     * @param topic 写入的kafka主题
     * @return 返回 sql 语句 : with( config of upsert_kafka_sink connector... )
     */
    public static String getUpsertKafkaSinkConfigSql(String topic){
        String sql = " WITH ( " +
                " 'connector' = 'upsert-kafka', "+
                " 'topic' = '%s', "+
                " 'properties.bootstrap.servers' = '%s', "+
                " 'key.format' = 'json', " +
                " 'value.format' = 'json' " +
                " ) ";

        return String.format(sql,topic,PropertyUtil.getStringValue("KAFKA_BROKERS"));
    }

    /**
     * 获取 doris sink 配置sql语句
     * @param table 要写入的表名
     * @return sql语句 with( config of doris connector... )
     */
    public static String getDorisSinkSql(String table){
        String sql =" WITH (" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '%s', " +
                " 'table.identifier' = '%s', " +
                " 'username' = '%s', " +
                " 'password' = '%s', " +
                " 'sink.properties.format' = 'json', " +
                " 'sink.properties.read_json_by_line' = 'true', " +
                " 'sink.buffer-count' = '100', " +
                " 'sink.buffer-flush.interval' = '1s', " +
                " 'sink.enable-2pc' = 'false' " +   // 测试阶段可以关闭两阶段提交,方便测试
                " ); ";
        return String.format(sql,
                PropertyUtil.getStringValue("DORIS_FE"),
                table,
                PropertyUtil.getStringValue("DORIS_USER"),
                PropertyUtil.getStringValue("DORIS_PASSWORD")
        );

    }
}

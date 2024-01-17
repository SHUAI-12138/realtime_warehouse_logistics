package com.atguigu.realtime.base;

import com.atguigu.realtime.util.PropertyUtil;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public abstract class BaseSqlApp {

    /**
     * 程序开始执行的入口
     * @param port 任务分配的端口， 可以在Web端访问
     * @param parallelism 任务并行度
     * @param jobName 任务名称
     */
    public void start(int port, int parallelism, String jobName) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        conf.setString("pipeline.name",jobName);
        // 设置水印推进策略
        conf.setString("table.exec.source.idle-timeout", "10s");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(parallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        env.enableCheckpointing(PropertyUtil.getIntegerValue("CHECK_POINTING_INTERVAL"));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://" + PropertyUtil.getStringValue("CHECK_POINT_STORAGE") + ":8020/ck/logistics/"+jobName);

        //设置ck的超时时间
        checkpointConfig.setCheckpointTimeout(5 * 6000);
        //两次ck之间，必须间隔500ms
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);
        //设置ck的最大的并发数. 非对齐，强制为1
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //如果开启的是Barrier对齐，那么当60s还没有对齐完成，自动转换为非对齐
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(60));
        //默认情况，Job被cancel了，存储在外部设备的ck数据会自动删除。你可以设置永久持久化。
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //一旦ck失败到达10次，此时job就终止
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 10. job 失败的时候重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 对流做一些操作, 父类无法知道,只有子类才知道.
        handle(tableEnv,env);
    }

    protected abstract void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env);

    /**
     * 读取 ods_db 中的数据， 映射在 ods_db 表中
     * @param tableEnv table环境
     * @param groupId 消费者组 id
     */
    public void createOdsDB(StreamTableEnvironment tableEnv, String groupId){
        String sql = " create table ods_db ( " +
                " `database` string, " +
                " `table` string, " +
                " `type` string, " +
                " `ts` bigint, " +
                " `data` map<string,string>, " +
                " `old` map<string,string>, " +
                " pt as PROCTIME(), " + // 处理时间
                " et as TO_TIMESTAMP_LTZ(ts,3), " + // 事件时间
                " WATERMARK FOR et as et - INTERVAL '0.001' SECOND " + // 水印
                " ) " + SqlUtil.getKafkaSourceConfigSql(PropertyUtil.getStringValue("TOPIC_ODS_DB"), groupId);
        tableEnv.executeSql(sql);
    }

    /**
     * 从hbase中 获取 dim_base_dic 中的数据， 映射在 dim_dic 中
     * @param tableEnv table环境
     */
    public void createDicCode(StreamTableEnvironment tableEnv) {
        String sql = " create table dim_dic ( " +
                " id STRING, " +
                " info Row<dic_name STRING >, " +
                " PRIMARY KEY (id) NOT ENFORCED  " +
                " ) with ( " +
                " 'connector' = 'hbase-2.2',  " +
                " 'zookeeper.quorum' = 'project80:2181'," +
                " 'table-name' = 'gmall:dim_base_dic'," +
                " 'lookup.async' = 'true', " +
                " 'lookup.cache' = 'PARTIAL', " +
                " 'lookup.partial-cache.max-rows' = '100', " +
                " 'lookup.partial-cache.expire-after-write' = '1d', " +
                " 'lookup.partial-cache.cache-missing-key' = 'true'  " +
                " ) ";
        tableEnv.executeSql(sql);
    }
}

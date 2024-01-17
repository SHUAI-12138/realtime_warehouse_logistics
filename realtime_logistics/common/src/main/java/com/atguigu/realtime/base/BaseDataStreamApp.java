package com.atguigu.realtime.base;

import com.atguigu.realtime.util.KafkaUtil;
import com.atguigu.realtime.util.PropertyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public abstract class BaseDataStreamApp {

    /**
     * 调用 start 方法， 启动App
     * @param jobName job的名字
     * @param webPort flink WebUI 的端口号
     * @param parallelism 并行度
     * @param topic 消费的 kafka 的主题
     * @param groupId 消费者组id
     */
    public void start(String jobName, int webPort, int parallelism, String topic, String groupId) {
        Configuration conf = new Configuration();
        // web UI 端口号
        conf.setInteger("rest.port", webPort);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 设置并行度
        env.setParallelism(parallelism);
        // 设置 check_point 的保存周期
        env.enableCheckpointing(PropertyUtil.getIntegerValue("CHECK_POINTING_INTERVAL"));
        // 设置 checkpoint 的保存路径, 因为保存在 hdfs 上， 所以 必须保证 hadoop 是开启的 , 用户问题
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://" + PropertyUtil.getStringValue("CHECK_POINT_STORAGE") + ":8020/ck/logistics/"+jobName);
        //checkpointConfig.setCheckpointStorage("file:///e:/tmp/ck/"+jobName);
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

        //从kafka的某个主题中读取数据
        KafkaSource<String> source = KafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

        handle(env, ds);

        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds);
}

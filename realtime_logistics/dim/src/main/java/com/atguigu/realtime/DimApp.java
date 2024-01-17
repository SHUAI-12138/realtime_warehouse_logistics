package com.atguigu.realtime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.function.HBaseSinkFunction;
import com.atguigu.realtime.util.HBaseUtil;
import com.atguigu.realtime.util.JDBCUtil;
import com.atguigu.realtime.util.PropertyUtil;
import com.google.common.collect.Sets;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Admin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Slf4j
public class DimApp extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DimApp()
                .start(
                        "dim",
                        14001,
                        1,
                        PropertyUtil.getStringValue("TOPIC_ODS_DB"),
                        "dim"
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //  清洗数据
        SingleOutputStreamOperator<JSONObject> etled = etl(ds);
        // 获取 dim 表的配置信息
        SingleOutputStreamOperator<TableProcess> configInfo = getConfig(env);
        // 在 hbase 中创建表
        SingleOutputStreamOperator<TableProcess> config = createTableInHBase(configInfo);
        // 关联两个流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connected = connectTwoStream(etled, config);
        // 过滤 字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filtered = dropFields(connected);
        // 写入到 HBase 和 redis 中
        filtered.addSink(new HBaseSinkFunction());
        // filtered.print();
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dropFields(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connected) {
        return connected
                .map(data -> {
                    JSONObject jsonObject = data.f0;
                    TableProcess tableProcess = data.f1;
                    String sinkColumns = tableProcess.getSinkColumns() + ",type,ts";
                    String[] columns = sinkColumns.split(",");
                    JSONObject res = new JSONObject();
                    for(String col : columns) {
                        res.put(col, jsonObject.getString(col));
                    }
                    return Tuple2.of(res, tableProcess);
                }).returns(Types.TUPLE(TypeInformation.of(JSONObject.class), TypeInformation.of(TableProcess.class)));
    }

    private SingleOutputStreamOperator<TableProcess> createTableInHBase(SingleOutputStreamOperator<TableProcess> configInfo) {
        return configInfo
                .map(new RichMapFunction<TableProcess, TableProcess>() {
                    // admin 线程不安全 每个 task 都应该有自己的 admin
                    Admin admin;
                    String namespace;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        admin = HBaseUtil.connection.getAdmin();
                        namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
                    }

                    @Override
                    public TableProcess map(TableProcess value) throws Exception {
                        String op = value.getOp();
                        switch (op) {
                            case "d":
                                HBaseUtil.dropTable(admin, namespace, value.getSinkTable());
                                break;
                            case "u":
                                HBaseUtil.dropTable(admin, namespace, value.getSinkTable());
                                HBaseUtil.createTable(admin, namespace, value.getSinkTable(), value.getSinkFamily());
                                break;
                            case "c":
                            case "r":
                                HBaseUtil.createTable(admin, namespace, value.getSinkTable(), value.getSinkFamily());
                                break;
                        }
                        return value;
                    }
                });
    }

    /**
     * 关联数据流 和 配置流
     * @param etled 数据流
     * @param config 配置流
     * @return 关联后的流
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectTwoStream(SingleOutputStreamOperator<JSONObject> etled, SingleOutputStreamOperator<TableProcess> config) {
        MapStateDescriptor<String, TableProcess> dimConfig = new MapStateDescriptor<>("dim_config", Types.STRING, TypeInformation.of(TableProcess.class));
        // 将配置流制作成 广播流
        BroadcastStream<TableProcess> broadcast = config.broadcast(dimConfig);
        return etled
                .connect(broadcast)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    Map<String, TableProcess> configMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        configMap = new HashMap<>();
                        List<TableProcess> tableProcesses = JDBCUtil.queryBeanList(" select * from table_process where sink_type = 'DIM' ", TableProcess.class);
                        tableProcesses
                                .stream()
                                .forEach(data -> {
                                    data.setOp("r");
                                    configMap.put(data.getSourceTable(), data);
                                });
                    }

                    @Override
                    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(dimConfig);
                        String table = value.getString("table");
                        // String database = value.getString("database");
                        JSONObject data = value.getJSONObject("data");
                        String ts = value.getString("ts");
                        String type = value.getString("type").replace("bootstrap-", "");
                        TableProcess tableProcess = broadcastState.get(table);
                        if (tableProcess == null) {
                            tableProcess = configMap.get(table);
                        }
                        if (tableProcess != null) {
                            data.put("ts", ts);
                            data.put("type", type);
                            data.put("table", table);
                            out.collect(Tuple2.of(data, tableProcess));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess value, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(dimConfig);
                        String op = value.getOp();
                        String table = value.getSourceTable();
                        if (op.equals("d")) {
                            broadcastState.remove(table);
                            configMap.remove(table);
                        } else {
                            broadcastState.put(table, value);
                            configMap.put(table, value);
                        }
                    }
                });
    }

    /**
     * 获取 配置流
     * @param env stream 环境
     * @return 返回配置流
     */
    private SingleOutputStreamOperator<TableProcess> getConfig(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(PropertyUtil.getStringValue("MYSQL_HOST"))
                .port(PropertyUtil.getIntegerValue("MYSQL_PORT"))
                .databaseList(PropertyUtil.getStringValue("CONFIG_DATABASE"))
                .tableList(PropertyUtil.getStringValue("CONFIG_DATABASE") + "." + PropertyUtil.getStringValue("CONFIG_TABLE"))
                .username(PropertyUtil.getStringValue("MYSQL_USER"))
                .password(PropertyUtil.getStringValue("MYSQL_PASSWORD"))
                .startupOptions(StartupOptions.initial())
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        // 如果想要遵循顺序，需要设置为一个并行度
        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "config_dim").setParallelism(1)
                .map(str -> {
                    JSONObject jsonObject = JSON.parseObject(str);
                    String op = jsonObject.getString("op");
                    TableProcess res;
                    if (op.equals("d")) {
                        res = JSON.parseObject(jsonObject.getString("before"), TableProcess.class);
                    } else {
                        res = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
                    }
                    res.setOp(op);
                    return res;
                }).setParallelism(1)
                .filter(data -> data.getSinkType().equals("DIM"));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds) {
        HashSet<String> operations = Sets.<String>newHashSet("delete", "insert", "update", "bootstrap-insert");
        return ds
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = null;
                        // 如果 数据流 不是json格式， 捕获到异常， 直接返回，不收集
                        try {
                            jsonObject = JSON.parseObject(value);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                            return;
                        }

                        String table = jsonObject.getString("table");
                        String data = jsonObject.getString("data");
                        String database = jsonObject.getString("database");
                        String type = jsonObject.getString("type");
                        String ts = jsonObject.getString("ts");

                        if (
                                operations.contains(type)
                                        && PropertyUtil.getStringValue("BUSI_DATABASE").equals(database)
                                        && StringUtils.isNoneBlank(ts, table, data)
                                        && data.length() > 2
                        ) {
                            out.collect(jsonObject);
                        }
                    }
                });
    }
}

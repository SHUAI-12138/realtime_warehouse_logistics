package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.KafkaUtil;
import com.atguigu.realtime.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRADE_ORDER_ORG;

@Slf4j
public class DwdTradeOrderOrg extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwdTradeOrderOrg()
                .start(
                        TOPIC_DWD_TRADE_ORDER_ORG,
                        15507,
                        4,
                        PropertyUtil.getStringValue("TOPIC_ODS_DB"),
                        TOPIC_DWD_TRADE_ORDER_ORG
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        // 准备好测流，收集要关联的表的数据
        OutputTag<JSONObject> order_info = new OutputTag<>("order_info", TypeInformation.of(JSONObject.class));
        // 获取需要的数据
        SingleOutputStreamOperator<JSONObject> orderOrg = etl(ds, order_info);
        SideOutputDataStream<JSONObject> orderInfo = orderOrg.getSideOutput(order_info);
        //orderInfo.printToErr();
        //orderOrg.print();
        // connect
        SingleOutputStreamOperator<JSONObject> connected = connectInfoAndOrg(orderOrg, orderInfo);
        // 写出到 kafka
        writeToKafka(connected);
        //connected.print();
    }

    private void writeToKafka(SingleOutputStreamOperator<JSONObject> connected) {
        connected
                .map(data -> data.toJSONString())
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRADE_ORDER_ORG));
    }

    private SingleOutputStreamOperator<JSONObject> connectInfoAndOrg(SingleOutputStreamOperator<JSONObject> orderOrg, SideOutputDataStream<JSONObject> orderInfo) {
        ConnectedStreams<JSONObject, JSONObject> connect = orderOrg.connect(orderInfo);
        return connect
                .keyBy(e -> e.getString("order_id"), e -> e.getString("id"), Types.STRING)
                .process(new KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject>() {
                    ValueState<JSONObject> orderInfo;
                    ListState<JSONObject> orderOrgBound;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderInfo = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("orderInfo", TypeInformation.of(JSONObject.class)));
                        orderOrgBound = getRuntimeContext().getListState(new ListStateDescriptor<JSONObject>("orderOrgBound", TypeInformation.of(JSONObject.class)));
                    }

                    @Override
                    public void processElement1(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject order_info = orderInfo.value();
                        if(order_info != null) {
                            value.putAll(order_info);
                            out.collect(value);
                        } else {
                            orderOrgBound.add(value);
                        }
                    }

                    @Override
                    public void processElement2(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        orderInfo.update(value);
                        if(orderOrgBound.get() == null) return;
                        for (JSONObject jsonObject : orderOrgBound.get()) {
                            value.putAll(jsonObject);
                            out.collect(value);
                        }
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds, OutputTag<JSONObject> order_info) {
         return ds
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if (!jsonObject.getString("database").equals(PropertyUtil.getStringValue("BUSI_DATABASE"))) return;
                            String table = jsonObject.getString("table");
                            if (!table.equals("order_info") && !table.equals("order_org_bound")) return;
                            // 只要 insert 的数据， 防止重复
                            if (!jsonObject.getString("type").replace("bootstrap-", "").equals("insert")) return;
                            JSONObject data = jsonObject.getJSONObject("data");
                            if (table.equals("order_info")) {
                                JSONObject res = new JSONObject();
                                String[] cols = {"id", "amount", "cargo_num"};
                                for(String col : cols) res.put(col, data.getString(col));
                                ctx.output(order_info, res);
                            } else {
                                data.remove("id");
                                data.put("ts", DateFormatUtil.dateTimeToTs(data.getString("create_time")));
                                data.remove("inbound_emp_id");
                                data.remove("is_deleted");
                                data.remove("inbound_time");
                                data.remove("create_time");
                                out.collect(data);
                            }
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

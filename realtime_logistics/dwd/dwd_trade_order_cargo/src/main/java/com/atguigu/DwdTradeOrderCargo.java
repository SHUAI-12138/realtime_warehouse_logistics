package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.pojo.OrderInfo;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.KafkaUtil;
import com.atguigu.realtime.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
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

import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRADE_ORDER_CARGO;

@Slf4j
public class DwdTradeOrderCargo extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwdTradeOrderCargo()
                .start(
                        TOPIC_DWD_TRADE_ORDER_CARGO,
                        15504,
                        4,
                        PropertyUtil.getStringValue("TOPIC_ODS_DB"),
                        TOPIC_DWD_TRADE_ORDER_CARGO
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        OutputTag<OrderInfo> orderInfo = new OutputTag<>("order_info", TypeInformation.of(OrderInfo.class));
        // 过滤出需要的数据
        SingleOutputStreamOperator<JSONObject> orderCargo = etl(ds, orderInfo);
        SideOutputDataStream<OrderInfo> order = orderCargo.getSideOutput(orderInfo);
        // connect
        SingleOutputStreamOperator<JSONObject> connected = connectCargoAndInfo(orderCargo, order);
        //
        writeToKafka(connected);
        // connected.print();
    }

    private void writeToKafka(SingleOutputStreamOperator<JSONObject> connected) {
        connected
                .map(data -> data.toJSONString())
                //.print();
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRADE_ORDER_CARGO));
    }

    private SingleOutputStreamOperator<JSONObject> connectCargoAndInfo(SingleOutputStreamOperator<JSONObject> orderCargo, SideOutputDataStream<OrderInfo> order) {
        ConnectedStreams<JSONObject, OrderInfo> connected = orderCargo.connect(order);
        return connected
                .keyBy(e -> e.getString("order_id"), OrderInfo::getId, Types.STRING)
                .process(new KeyedCoProcessFunction<String, JSONObject, OrderInfo, JSONObject>() {
                    @Override
                    public void processElement1(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject join = other.value();
                        if (join == null) other.update(value);
                        else {
                            value.putAll(join);
                            out.collect(value);
                            other.update(null);
                        }
                    }

                    ValueState<JSONObject> other;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        other = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("other", TypeInformation.of(JSONObject.class)));
                    }

                    @Override
                    public void processElement2(OrderInfo value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject join = other.value();
                        SerializeConfig config = new SerializeConfig();
                        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
                        JSONObject jsonObject = JSON.parseObject(JSON.toJSONString(value, config));
                        if (join == null) {
                            other.update(jsonObject);
                        }
                        else {
                            jsonObject.putAll(join);
                            out.collect(jsonObject);
                            other.update(null);
                        }
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds, OutputTag<OrderInfo> orderInfo) {
         return ds
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if (!jsonObject.getString("database").equals(PropertyUtil.getStringValue("BUSI_DATABASE"))) return;
                            String table = jsonObject.getString("table");
                            if(!"order_cargo".equals(table) && !"order_info".equals(table)) return;
                            if(!"insert".equals(jsonObject.getString("type").replace("bootstrap-", ""))) return;
                            JSONObject data = jsonObject.getJSONObject("data");
                            data.put("ts", DateFormatUtil.dateTimeToTs(data.getString("create_time")));
                            if(table.equals("order_cargo")) {
                                data.remove("id");
                                out.collect(data);
                            } else {
                                ctx.output(orderInfo, JSON.parseObject(data.toJSONString(), OrderInfo.class));
                            }
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

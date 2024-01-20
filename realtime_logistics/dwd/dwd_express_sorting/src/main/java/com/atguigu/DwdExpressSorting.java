package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.function.AsyncLookUpJoinFunction;
import com.atguigu.realtime.function.JoinRegionFunction;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.KafkaUtil;
import com.atguigu.realtime.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_EXPRESS_SORTING;

@Slf4j
public class DwdExpressSorting extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwdExpressSorting()
                .start(
                        TOPIC_DWD_EXPRESS_SORTING,
                        15501,
                        4,
                        PropertyUtil.getStringValue("TOPIC_ODS_DB"),
                        TOPIC_DWD_EXPRESS_SORTING
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        // 过滤出需要的数据
        SingleOutputStreamOperator<JSONObject> etled = etl(ds);
        // 关联维度字段
        SingleOutputStreamOperator<JSONObject> joined = joinDim(etled);
        // 写入到kafka
        writeToKafka(joined);
        // ds.print();
    }

    private SingleOutputStreamOperator<JSONObject> joinDim(SingleOutputStreamOperator<JSONObject> etled) {
        SingleOutputStreamOperator<JSONObject> orgId = AsyncDataStream
                .orderedWait(
                        etled,
                        new AsyncLookUpJoinFunction<JSONObject>("base_organ") {
                            @Override
                            public String getIdValue(JSONObject value) {
                                return value.getString("org_id");
                            }

                            @Override
                            protected void extractDimData(JSONObject value, JSONObject dimData) {
                                value.put("region_id", dimData.getString("region_id"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );
        return orgId.map(new JoinRegionFunction());
    }

    private void writeToKafka(SingleOutputStreamOperator<JSONObject> etled) {
        etled
                .map(data -> data.toJSONString())
                // .print();
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_EXPRESS_SORTING));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if (!jsonObject.getString("database").equals(PropertyUtil.getStringValue("BUSI_DATABASE"))) return;
                            if (!jsonObject.getString("table").equals("order_org_bound")) return;
                            JSONObject data = jsonObject.getJSONObject("data");
                            if (data.getString("status").equals("64003")) {
                                data.put("ts", DateFormatUtil.dateTimeToTs(data.getString("sort_time")));
                                out.collect(data);
                            }
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.KafkaUtil;
import com.atguigu.realtime.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_ORDER_STATUS_COUNT;

@Slf4j
public class DwdOrderStatusCount extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwdOrderStatusCount()
                .start(
                        TOPIC_DWD_ORDER_STATUS_COUNT,
                        15506,
                        4,
                        PropertyUtil.getStringValue("TOPIC_ODS_DB"),
                        TOPIC_DWD_ORDER_STATUS_COUNT
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<JSONObject> etled = etl(ds);
        //
        writeToKafka(etled);
        //etled.print();
    }

    private void writeToKafka(SingleOutputStreamOperator<JSONObject> etled) {
        etled
                .map(data -> data.toJSONString())
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_ORDER_STATUS_COUNT));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            String database = jsonObject.getString("database");
                            if (!database.equals(PropertyUtil.getStringValue("BUSI_DATABASE"))) return;
                            if (!jsonObject.getString("table").equals("order_info")) return;
                            JSONObject data = jsonObject.getJSONObject("data");
                            String status = data.getString("status");
                            // 60030 已取件（接单） 60050 运送中（发单） 60060 派送中（转运完成）
                            if(!"60030".equals(status) && !"60050".equals(status) && !"60060".equals(status)) return;
                            data.put("ts", DateFormatUtil.dateTimeToTs(data.getString("update_time")));
                            JSONObject res = new JSONObject();
                            String[] cols = {"ts", "status", "order_no", "update_time", "create_time"};
                            for(String col : cols) res.put(col, data.getString(col));
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

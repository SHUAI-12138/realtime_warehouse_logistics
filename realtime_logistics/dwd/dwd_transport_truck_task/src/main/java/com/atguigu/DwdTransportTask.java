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

import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRANSPORT_TRUCK_TASK;

@Slf4j
public class DwdTransportTask extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwdTransportTask()
                .start(
                        TOPIC_DWD_TRANSPORT_TRUCK_TASK,
                        15508,
                        4,
                        PropertyUtil.getStringValue("TOPIC_ODS_DB"),
                        TOPIC_DWD_TRANSPORT_TRUCK_TASK
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
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRANSPORT_TRUCK_TASK));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if (!jsonObject.getString("database").equals(PropertyUtil.getStringValue("BUSI_DATABASE"))) return;
                            if (!jsonObject.getString("table").equals("transport_task")) return;
                            JSONObject data = jsonObject.getJSONObject("data");
                            if(!"67004".equals(data.getString("status"))) return;
                            String[] cols = {"actual_distance", "actual_end_time", "actual_start_time", "start_org_id", "start_org_name", "truck_id", "order_num", "end_org_id", "end_org_name"};
                            JSONObject res = new JSONObject();
                            for(String col : cols) res.put(col, data.getString(col));
                            res.put("ts", data.getString("update_time"));
                            res.put("diff_time", DateFormatUtil.dateTimeToTs(data.getString("actual_end_time")) - DateFormatUtil.dateTimeToTs(data.getString("actual_start_time")));
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

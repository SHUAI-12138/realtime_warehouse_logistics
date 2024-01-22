package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.pojo.TransportOrg;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.function.AsyncLookUpJoinFunction;
import com.atguigu.realtime.function.DorisMapFunction;
import com.atguigu.realtime.function.JoinRegionFunction;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.DorisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_TRANSPORT_CITY_ORG_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRANSPORT_TRUCK_TASK;

@Slf4j
public class DwsTransportOrgWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsTransportOrgWindow()
                .start(
                        DWS_TRANSPORT_CITY_ORG_VIEW_WINDOW,
                        16506,
                        4,
                        TOPIC_DWD_TRANSPORT_TRUCK_TASK,
                        DWS_TRANSPORT_CITY_ORG_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<TransportOrg> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<TransportOrg> agged = agg(toPojo);
        //
        SingleOutputStreamOperator<TransportOrg> joined = joinDim(agged);
        //
        writeToDoris(joined);
        //joined.print();
    }

    private void writeToDoris(SingleOutputStreamOperator<TransportOrg> joined) {
        joined
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_TRANSPORT_CITY_ORG_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<TransportOrg> joinDim(SingleOutputStreamOperator<TransportOrg> agged) {
        SingleOutputStreamOperator<JSONObject> orgId = agged.map(new DorisMapFunction<>()).map(data -> JSON.parseObject(data));
        SingleOutputStreamOperator<JSONObject> regionId = AsyncDataStream
                .orderedWait(
                        orgId,
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
        return regionId.map(new JoinRegionFunction()).map(data -> JSON.parseObject(data.toJSONString(), TransportOrg.class));
    }

    private SingleOutputStreamOperator<TransportOrg> agg(SingleOutputStreamOperator<TransportOrg> toPojo) {
        WatermarkStrategy<TransportOrg> watermarkStrategy = WatermarkStrategy.<TransportOrg>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(TransportOrg::getOrgId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TransportOrg>() {
                    @Override
                    public TransportOrg reduce(TransportOrg value1, TransportOrg value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<TransportOrg, TransportOrg, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TransportOrg> elements, Collector<TransportOrg> out) throws Exception {
                        TimeWindow window = context.window();
                        TransportOrg next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<TransportOrg> parseToPojo(DataStreamSource<String> ds) {
        return ds
                .process(new ProcessFunction<String, TransportOrg>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TransportOrg> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            TransportOrg res = new TransportOrg();
                            res.setOrgId(jsonObject.getString("start_org_id"));
                            res.setOrgName(jsonObject.getString("start_org_name"));
                            res.setTs(jsonObject.getLong("ts"));
                            res.setTransDistance(jsonObject.getDouble("actual_distance"));
                            res.setTransTime(jsonObject.getLong("diff_time")/1000);
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

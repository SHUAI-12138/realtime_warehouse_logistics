package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.pojo.TransportTruck;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.function.AsyncLookUpJoinFunction;
import com.atguigu.realtime.function.DorisMapFunction;
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

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_TRANSPORT_TRUCK_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRANSPORT_TRUCK_TASK;

@Slf4j
public class DwsTransportTruckWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsTransportTruckWindow()
                .start(
                        DWS_TRANSPORT_TRUCK_VIEW_WINDOW,
                        16507,
                        4,
                        TOPIC_DWD_TRANSPORT_TRUCK_TASK,
                        DWS_TRANSPORT_TRUCK_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<TransportTruck> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<TransportTruck> agged = agg(toPojo);
        //
        SingleOutputStreamOperator<TransportTruck> joined = joinDim(agged);
        //
        writeToDoris(joined);
        //joined.print();
    }

    private void writeToDoris(SingleOutputStreamOperator<TransportTruck> joined) {
        joined
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_TRANSPORT_TRUCK_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<TransportTruck> joinDim(SingleOutputStreamOperator<TransportTruck> agged) {
        return AsyncDataStream
                .orderedWait(
                        agged,
                        new AsyncLookUpJoinFunction<TransportTruck>("truck_model") {
                            @Override
                            public String getIdValue(TransportTruck value) {
                                return value.getTruckModeId();
                            }

                            @Override
                            protected void extractDimData(TransportTruck value, JSONObject dimData) {
                                value.setTruckModeType(dimData.getString("model_type"));
                                value.setTruckModeName(dimData.getString("model_name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );
    }

    private SingleOutputStreamOperator<TransportTruck> agg(SingleOutputStreamOperator<TransportTruck> toPojo) {
        WatermarkStrategy<TransportTruck> watermarkStrategy = WatermarkStrategy.<TransportTruck>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(TransportTruck::getTruckModeId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TransportTruck>() {
                    @Override
                    public TransportTruck reduce(TransportTruck value1, TransportTruck value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<TransportTruck, TransportTruck, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TransportTruck> elements, Collector<TransportTruck> out) throws Exception {
                        TimeWindow window = context.window();
                        TransportTruck next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<TransportTruck> parseToPojo(DataStreamSource<String> ds) {
        SingleOutputStreamOperator<TransportTruck> pojoEd = ds
                .process(new ProcessFunction<String, TransportTruck>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TransportTruck> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            TransportTruck res = new TransportTruck();
                            res.setTs(jsonObject.getLong("ts"));
                            res.setTruckId(jsonObject.getString("truck_id"));
                            res.setTransDistance(jsonObject.getDouble("actual_distance"));
                            res.setTransTime(jsonObject.getLong("diff_time"));
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
        return AsyncDataStream
                .orderedWait(
                        pojoEd,
                        new AsyncLookUpJoinFunction<TransportTruck>("truck_info") {
                            @Override
                            public String getIdValue(TransportTruck value) {
                                return value.getTruckId();
                            }

                            @Override
                            protected void extractDimData(TransportTruck value, JSONObject dimData) {
                                value.setTruckModeId(dimData.getString("truck_model_id"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );
    }
}

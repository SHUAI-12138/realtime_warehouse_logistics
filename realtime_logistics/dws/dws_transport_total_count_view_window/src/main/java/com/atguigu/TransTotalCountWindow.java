package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.pojo.TransportTotal;
import com.atguigu.realtime.base.BaseDataStreamApp;
import com.atguigu.realtime.function.DorisMapFunction;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.DorisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static com.atguigu.realtime.constant.LogisticsConstant.*;

@Slf4j
public class TransTotalCountWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new TransTotalCountWindow()
                .start(
                        DWS_TRANSPORT_TOTAL_COUNT_VIEW_WINDOW,
                        16502,
                        4,
                        TOPIC_DWD_TRANSPORT_TRUCK_TASK,
                        DWS_TRANSPORT_TOTAL_COUNT_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<TransportTotal> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<TransportTotal> agged = agg(toPojo);
        //
        writeToDoris(agged);
        //agged.print();
    }

    private void writeToDoris(SingleOutputStreamOperator<TransportTotal> agged) {
        agged
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_TRANSPORT_TOTAL_COUNT_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<TransportTotal> agg(SingleOutputStreamOperator<TransportTotal> toPojo) {
        WatermarkStrategy<TransportTotal> watermarkStrategy = WatermarkStrategy.<TransportTotal>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TransportTotal>() {
                    @Override
                    public TransportTotal reduce(TransportTotal value1, TransportTotal value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new AllWindowFunction<TransportTotal, TransportTotal, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TransportTotal> values, Collector<TransportTotal> out) throws Exception {
                        TransportTotal next = values.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<TransportTotal> parseToPojo(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, TransportTotal>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TransportTotal> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            TransportTotal res = JSON.parseObject(value, TransportTotal.class);
                            res.setTransDistance(jsonObject.getDouble("actual_distance"));
                            res.setTransTime(jsonObject.getLong("diff_time") / 1000);
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.ExpressDeliver;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static com.atguigu.realtime.constant.LogisticsConstant.*;

@Slf4j
public class DwsExpressDeliverWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsExpressDeliverWindow()
                .start(
                        DWS_EXPRESS_DELIVER_VIEW_WINDOW,
                        16510,
                        4,
                        TOPIC_DWD_EXPRESS_DELIVER,
                        DWS_EXPRESS_DELIVER_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<ExpressDeliver> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<ExpressDeliver> agged = agg(toPojo);
        //
        writeToDoris(agged);
    }

    private void writeToDoris(SingleOutputStreamOperator<ExpressDeliver> agged) {
        agged
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_EXPRESS_DELIVER_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<ExpressDeliver> agg(SingleOutputStreamOperator<ExpressDeliver> toPojo) {
        WatermarkStrategy<ExpressDeliver> watermarkStrategy = WatermarkStrategy.<ExpressDeliver>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ExpressDeliver::getOrgId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<ExpressDeliver>() {
                    @Override
                    public ExpressDeliver reduce(ExpressDeliver value1, ExpressDeliver value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<ExpressDeliver, ExpressDeliver, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ExpressDeliver> elements, Collector<ExpressDeliver> out) throws Exception {
                        TimeWindow window = context.window();
                        ExpressDeliver next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<ExpressDeliver> parseToPojo(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, ExpressDeliver>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ExpressDeliver> out) throws Exception {
                        try {
                            ExpressDeliver res = JSON.parseObject(value, ExpressDeliver.class);
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

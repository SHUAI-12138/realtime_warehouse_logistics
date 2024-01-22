package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.ExpressSorting;
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

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_EXPRESS_SORTING_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_EXPRESS_SORTING;

@Slf4j
public class DwsExpressSortingWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsExpressSortingWindow()
                .start(
                        DWS_EXPRESS_SORTING_VIEW_WINDOW,
                        16508,
                        4,
                        TOPIC_DWD_EXPRESS_SORTING,
                        DWS_EXPRESS_SORTING_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<ExpressSorting> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<ExpressSorting> agged = agg(toPojo);
        //
        writeToDoris(agged);
    }

    private void writeToDoris(SingleOutputStreamOperator<ExpressSorting> agged) {
        agged
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_EXPRESS_SORTING_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<ExpressSorting> agg(SingleOutputStreamOperator<ExpressSorting> toPojo) {
        WatermarkStrategy<ExpressSorting> watermarkStrategy = WatermarkStrategy.<ExpressSorting>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ExpressSorting::getOrgId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<ExpressSorting>() {
                    @Override
                    public ExpressSorting reduce(ExpressSorting value1, ExpressSorting value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<ExpressSorting, ExpressSorting, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ExpressSorting> elements, Collector<ExpressSorting> out) throws Exception {
                        TimeWindow window = context.window();
                        ExpressSorting next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<ExpressSorting> parseToPojo(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, ExpressSorting>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ExpressSorting> out) throws Exception {
                        try {
                            ExpressSorting expressSorting = JSON.parseObject(value, ExpressSorting.class);
                            out.collect(expressSorting);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.ExpressPickup;
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

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_EXPRESS_PICKUP_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_EXPRESS_PICKUP;

@Slf4j
public class DwsExpressPickupWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsExpressPickupWindow()
                .start(
                        DWS_EXPRESS_PICKUP_VIEW_WINDOW,
                        16509,
                        4,
                        TOPIC_DWD_EXPRESS_PICKUP,
                        DWS_EXPRESS_PICKUP_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<ExpressPickup> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<ExpressPickup> agged = agg(toPojo);
        //
        writeToDoris(agged);
    }

    private void writeToDoris(SingleOutputStreamOperator<ExpressPickup> agged) {
        agged
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_EXPRESS_PICKUP_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<ExpressPickup> agg(SingleOutputStreamOperator<ExpressPickup> toPojo) {
        WatermarkStrategy<ExpressPickup> watermarkStrategy = WatermarkStrategy.<ExpressPickup>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ExpressPickup::getOrgId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<ExpressPickup>() {
                    @Override
                    public ExpressPickup reduce(ExpressPickup value1, ExpressPickup value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<ExpressPickup, ExpressPickup, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ExpressPickup> elements, Collector<ExpressPickup> out) throws Exception {
                        TimeWindow window = context.window();
                        ExpressPickup next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<ExpressPickup> parseToPojo(DataStreamSource<String> ds) {
        return ds
                .process(new ProcessFunction<String, ExpressPickup>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ExpressPickup> out) throws Exception {
                        try {
                            ExpressPickup expressSorting = JSON.parseObject(value, ExpressPickup.class);
                            out.collect(expressSorting);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.pojo.OrderStatus;
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

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_ORDER_STATUS_COUNT_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_ORDER_STATUS_COUNT;

@Slf4j
public class OrderStatusCountWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new OrderStatusCountWindow()
                .start(
                        DWS_ORDER_STATUS_COUNT_VIEW_WINDOW,
                        16501,
                        4,
                        TOPIC_DWD_ORDER_STATUS_COUNT,
                        DWS_ORDER_STATUS_COUNT_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<OrderStatus> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<OrderStatus> agged = agg(toPojo);
        //
        writeToDoris(agged);
        //agged.print();
    }

    private void writeToDoris(SingleOutputStreamOperator<OrderStatus> agged) {
        agged
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_ORDER_STATUS_COUNT_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<OrderStatus> agg(SingleOutputStreamOperator<OrderStatus> toPojo) {
        WatermarkStrategy<OrderStatus> watermarkStrategy = WatermarkStrategy.<OrderStatus>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<OrderStatus>() {
                    @Override
                    public OrderStatus reduce(OrderStatus value1, OrderStatus value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new AllWindowFunction<OrderStatus, OrderStatus, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<OrderStatus> values, Collector<OrderStatus> out) throws Exception {
                        OrderStatus next = values.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<OrderStatus> parseToPojo(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, OrderStatus>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<OrderStatus> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            String status = jsonObject.getString("status");
                            OrderStatus res = JSON.parseObject(value, OrderStatus.class);
                            switch (status) {
                                case "60030": res.setTackCount(1L); break;
                                case "60050": res.setSendCount(1L); break;
                                case "60060": res.setTcCount(1L);
                            }
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

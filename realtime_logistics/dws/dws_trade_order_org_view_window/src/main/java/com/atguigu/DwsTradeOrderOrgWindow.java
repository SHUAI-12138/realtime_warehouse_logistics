package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.pojo.OrderOrg;
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

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_TRADE_ORDER_ORG_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRADE_ORDER_ORG;

@Slf4j
public class DwsTradeOrderOrgWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsTradeOrderOrgWindow()
                .start(
                        DWS_TRADE_ORDER_ORG_VIEW_WINDOW,
                        16505,
                        4,
                        TOPIC_DWD_TRADE_ORDER_ORG,
                        DWS_TRADE_ORDER_ORG_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<OrderOrg> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<OrderOrg> agged = agg(toPojo);
        //
        SingleOutputStreamOperator<OrderOrg> joined = joinDim(agged);
        //
        writeToDoris(joined);
        //joined.print();
    }

    private void writeToDoris(SingleOutputStreamOperator<OrderOrg> joined) {
        joined
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_TRADE_ORDER_ORG_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<OrderOrg> joinDim(SingleOutputStreamOperator<OrderOrg> agged) {
        return AsyncDataStream
                .orderedWait(
                        agged,
                        new AsyncLookUpJoinFunction<OrderOrg>("base_organ") {
                            @Override
                            public String getIdValue(OrderOrg value) {
                                return value.getOrgId();
                            }

                            @Override
                            protected void extractDimData(OrderOrg value, JSONObject dimData) {
                                value.setOrgName(dimData.getString("org_name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );
    }

    private SingleOutputStreamOperator<OrderOrg> agg(SingleOutputStreamOperator<OrderOrg> toPojo) {
        WatermarkStrategy<OrderOrg> watermarkStrategy = WatermarkStrategy.<OrderOrg>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(OrderOrg::getOrgId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<OrderOrg>() {
                    @Override
                    public OrderOrg reduce(OrderOrg value1, OrderOrg value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<OrderOrg, OrderOrg, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<OrderOrg> elements, Collector<OrderOrg> out) throws Exception {
                        TimeWindow window = context.window();
                        OrderOrg next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<OrderOrg> parseToPojo(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, OrderOrg>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<OrderOrg> out) throws Exception {
                        try {
                            OrderOrg res = JSON.parseObject(value, OrderOrg.class);
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

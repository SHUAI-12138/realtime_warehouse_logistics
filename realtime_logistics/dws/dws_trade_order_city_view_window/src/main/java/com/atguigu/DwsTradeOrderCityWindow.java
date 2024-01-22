package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.pojo.OrderCity;
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

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_TRADE_ORDER_CITY_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRADE_ORDER_CARGO;

@Slf4j
public class DwsTradeOrderCityWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsTradeOrderCityWindow()
                .start(
                        DWS_TRADE_ORDER_CITY_VIEW_WINDOW,
                        16504,
                        4,
                        TOPIC_DWD_TRADE_ORDER_CARGO,
                        DWS_TRADE_ORDER_CITY_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<OrderCity> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<OrderCity> agged = agg(toPojo);
        //
        SingleOutputStreamOperator<OrderCity> joined = joinDim(agged);
        //
        writeToDoris(joined);
        //joined.print();
    }

    private void writeToDoris(SingleOutputStreamOperator<OrderCity> joined) {
        joined
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_TRADE_ORDER_CITY_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<OrderCity> joinDim(SingleOutputStreamOperator<OrderCity> agged) {
        SingleOutputStreamOperator<OrderCity> city = AsyncDataStream
                .orderedWait(
                        agged,
                        new AsyncLookUpJoinFunction<OrderCity>("base_region_info") {
                            @Override
                            public String getIdValue(OrderCity value) {
                                return value.getCityId();
                            }

                            @Override
                            protected void extractDimData(OrderCity value, JSONObject dimData) {
                                value.setCityName(dimData.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );
        return AsyncDataStream
                .orderedWait(
                        city,
                        new AsyncLookUpJoinFunction<OrderCity>("base_region_info") {
                            @Override
                            public String getIdValue(OrderCity value) {
                                return value.getProvinceId();
                            }

                            @Override
                            protected void extractDimData(OrderCity value, JSONObject dimData) {
                                value.setProvinceName(dimData.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );
    }

    private SingleOutputStreamOperator<OrderCity> agg(SingleOutputStreamOperator<OrderCity> toPojo) {
        WatermarkStrategy<OrderCity> watermarkStrategy = WatermarkStrategy.<OrderCity>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(OrderCity::getCityId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<OrderCity>() {
                    @Override
                    public OrderCity reduce(OrderCity value1, OrderCity value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<OrderCity, OrderCity, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<OrderCity> elements, Collector<OrderCity> out) throws Exception {
                        TimeWindow window = context.window();
                        OrderCity next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<OrderCity> parseToPojo(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, OrderCity>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<OrderCity> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            OrderCity res = new OrderCity();
                            res.setProvinceId(jsonObject.getString("sender_province_id"));
                            res.setCityId(jsonObject.getString("sender_city_id"));
                            res.setTs(jsonObject.getLong("ts"));
                            res.setTotalAmount(jsonObject.getDouble("amount"));
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

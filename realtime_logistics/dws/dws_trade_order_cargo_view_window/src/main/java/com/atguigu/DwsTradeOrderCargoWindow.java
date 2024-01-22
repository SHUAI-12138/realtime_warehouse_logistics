package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.pojo.TradeOrderCargo;
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

import static com.atguigu.realtime.constant.LogisticsConstant.DWS_TRADE_ORDER_CARGO_VIEW_WINDOW;
import static com.atguigu.realtime.constant.LogisticsConstant.TOPIC_DWD_TRADE_ORDER_CARGO;

@Slf4j
public class DwsTradeOrderCargoWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsTradeOrderCargoWindow()
                .start(
                        DWS_TRADE_ORDER_CARGO_VIEW_WINDOW,
                        16503,
                        4,
                        TOPIC_DWD_TRADE_ORDER_CARGO,
                        DWS_TRADE_ORDER_CARGO_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //
        SingleOutputStreamOperator<TradeOrderCargo> toPojo = parseToPojo(ds);
        //
        SingleOutputStreamOperator<TradeOrderCargo> agged = agg(toPojo);
        //
        SingleOutputStreamOperator<TradeOrderCargo> joined = joinDim(agged);
        writeToDoris(joined);
        //joined.print();
    }

    private SingleOutputStreamOperator<TradeOrderCargo> joinDim(SingleOutputStreamOperator<TradeOrderCargo> agged) {
         return AsyncDataStream
                .orderedWait(
                        agged,
                        new AsyncLookUpJoinFunction<TradeOrderCargo>("base_dic") {
                            @Override
                            public String getIdValue(TradeOrderCargo value) {
                                return value.getCargoType();
                            }

                            @Override
                            protected void extractDimData(TradeOrderCargo value, JSONObject dimData) {
                                value.setCargoName(dimData.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );
    }

    private void writeToDoris(SingleOutputStreamOperator<TradeOrderCargo> agged) {
        agged
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(DWS_TRADE_ORDER_CARGO_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<TradeOrderCargo> agg(SingleOutputStreamOperator<TradeOrderCargo> toPojo) {
        WatermarkStrategy<TradeOrderCargo> watermarkStrategy = WatermarkStrategy.<TradeOrderCargo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                .withIdleness(Duration.ofMinutes(10));
        return toPojo
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(TradeOrderCargo::getCargoType)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeOrderCargo>() {
                    @Override
                    public TradeOrderCargo reduce(TradeOrderCargo value1, TradeOrderCargo value2) throws Exception {
                        value1.sum(value2);
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeOrderCargo, TradeOrderCargo, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TradeOrderCargo> elements, Collector<TradeOrderCargo> out) throws Exception {
                        TimeWindow window = context.window();
                        TradeOrderCargo next = elements.iterator().next();
                        next.setStt(DateFormatUtil.parseTsToDateTime(window.getStart()));
                        next.setEdt(DateFormatUtil.parseTsToDateTime(window.getEnd()));
                        next.setCurDate(DateFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeOrderCargo> parseToPojo(DataStreamSource<String> ds) {
         return ds
                .process(new ProcessFunction<String, TradeOrderCargo>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<TradeOrderCargo> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            TradeOrderCargo res = JSON.parseObject(value, TradeOrderCargo.class);
                            res.setTotalAmount(jsonObject.getDouble("amount"));
                            out.collect(res);
                        } catch (Exception e) {
                            log.warn("json is illegal!");
                        }
                    }
                });
    }
}

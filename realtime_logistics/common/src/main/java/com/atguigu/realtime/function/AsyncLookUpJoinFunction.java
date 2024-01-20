package com.atguigu.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.util.HBaseUtil;
import com.atguigu.realtime.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

@Slf4j
public abstract class AsyncLookUpJoinFunction<T> extends DimAsyncOperateBaseFunction implements AsyncFunction<T,T>
{
    //关联哪个维度(业务表)
    private final String dimTable;

    /**
     * 获取 关联的字段的值
     * @param value 关联表的 row
     * @return row 中 关联字段的值
     */
    public abstract String getIdValue(T value);

    /**
     * 获取 要查询的表
     * @param dimTable 要查询的表名
     */
    public AsyncLookUpJoinFunction(String dimTable){
        this.dimTable = dimTable;
    }

    /**
     *
     * @param input element coming from an upstream task
     * @param resultFuture to be completed with the result data
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        CompletableFuture
                .supplyAsync(() -> getStringFromRedisAsync(dimTable, getIdValue(input)))
                .thenApplyAsync(data -> {
                    JSONObject res;
                    if(data == null) {
                        AsyncTable<AdvancedScanResultConsumer> table = tableMap.get(dimTable);
                        if(table == null) {
                            String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
                            table = HBaseUtil.getAsyncTable(namespace, "dim_" + dimTable);
                            tableMap.put(dimTable, table);
                        }
                        res = getValueFromHBaseAsync(table, getIdValue(input));
                        setStringToRedisAsync(dimTable, getIdValue(input), res.toJSONString());
                    } else {
                        res = JSON.parseObject(data);
                    }
                    return res;
                })
                .thenAccept(data -> {
                    extractDimData(input, data);
                    resultFuture.complete(Collections.singleton(input));
                });
    }

    /**
     * 将 读取到的维度数据添加进value中
     * @param value 最终的值
     * @param dimData 查询到的维度数据
     */
    protected abstract void extractDimData(T value, JSONObject dimData) ;
}
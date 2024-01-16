package com.atguigu.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DimAsyncOperateBaseFunction extends AbstractRichFunction {

    protected RedisClient asyncClint;
    protected StatefulRedisConnection<String, String> asyncRedisConnection;

    protected Map<String, AsyncTable<AdvancedScanResultConsumer>> tableMap = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncClint = AsyncRedisUtil.getAsyncRedisClient();
        asyncRedisConnection = AsyncRedisUtil.getAsyncRedisConnection(asyncClint);
        List<TableProcess> tableProcesses = JDBCUtil.queryBeanList(" select * from table_process where sink_type = 'DIM' ", TableProcess.class);
        String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
        tableProcesses.forEach(data -> {
            tableMap.put(data.getSourceTable(), HBaseUtil.getAsyncTable(namespace, data.getSinkTable()));
        });
    }

    @Override
    public void close() throws Exception {
        AsyncRedisUtil.closeAsyncConn(asyncRedisConnection);
        AsyncRedisUtil.closeAsyncClient(asyncClint);
    }

    /**
     * 读取 redis 中的 维度数据
     * @param table 表名
     * @param id 主键值
     * @return 维度数据
     */
    protected String getStringFromRedisAsync(String table, String id) {
        RedisFuture<String> stringRedisFuture = asyncRedisConnection.async().get(getRedisKey(table, id));
        try {
            return stringRedisFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * 向 redis 中写入数据
     * @param table 表名
     * @param id 主键值
     * @param value 要写入的值
     */
    protected void setStringToRedisAsync(String table, String id, String value) {
        String redisKey = getRedisKey(table, id);
        asyncRedisConnection.async().setex(redisKey, PropertyUtil.getIntegerValue("JEDIS_STRING_TTL"), value);
    }

    /**
     * 从 HBase 中 获取数据
     * @param table Table 对象
     * @param rowKey rowKey
     * @return 返回一个 JSONObject 对象
     */
    protected JSONObject getValueFromHBaseAsync(AsyncTable<AdvancedScanResultConsumer> table, String rowKey) {
        JSONObject res = new JSONObject();
        Get get = new Get(Bytes.toBytes(rowKey));
        CompletableFuture<Result> resultFuture = table.get(get);
        try {
            Cell[] cells = resultFuture.get().rawCells();
            for(Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                res.put(qualifier, value);
            }
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 向 Hbase 中 写入一行数据 data
     * @param rowKey rowKey
     * @param sinkFamily 列族
     * @param data 写入的数据
     * @return 返回一个 Put 对象
     */
    public static Put createPut(String rowKey, String sinkFamily, JSONObject data) {
        Put put = new Put(Bytes.toBytes(rowKey));
        byte[] family = Bytes.toBytes(sinkFamily);
        Set<String> fields = data.keySet();
        for(String field : fields) {
            String fieldValue = data.getString(field);
            if (fieldValue == null) fieldValue = "null";
            put.addColumn(
                    family,
                    Bytes.toBytes(field),
                    Bytes.toBytes(fieldValue)
            );
        }
        return put;
    }

    /**
     * 获取 redis 的 k
     * @param table 表名
     * @param id 主键值
     * @return k
     */
    protected String getRedisKey(String table, String id) {
        return table + ":" + id;
    }
}

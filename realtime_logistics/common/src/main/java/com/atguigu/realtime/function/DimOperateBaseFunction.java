package com.atguigu.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.HBaseUtil;
import com.atguigu.realtime.util.JDBCUtil;
import com.atguigu.realtime.util.PropertyUtil;
import com.atguigu.realtime.util.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class DimOperateBaseFunction extends AbstractRichFunction {

    protected Jedis jedis;
    protected Map<String, Table> tableMap = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = RedisUtil.getRedisClient();
        List<TableProcess> tableProcesses = JDBCUtil.queryBeanList(" select * from table_process where sink_type = 'DIM' ", TableProcess.class);
        String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
        tableProcesses.forEach(data -> {
            tableMap.put(data.getSourceTable(), HBaseUtil.getTable(namespace, data.getSinkTable()));
        });
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisClient(jedis);
        for(Map.Entry<String, Table> entry : tableMap.entrySet()) {
            entry.getValue().close();
        }
    }

    /**
     * 读取 redis 中的 维度数据
     * @param table 表名
     * @param id 主键值
     * @return 维度数据
     */
    protected String getStringFromRedis(String table, String id) {
        return jedis.get(getRedisKey(table, id));
    }

    /**
     * 向 redis 中写入数据
     * @param table 表名
     * @param id 主键值
     * @param value 要写入的值
     */
    protected void setStringToRedis(String table, String id, String value) {
        String redisKey = getRedisKey(table, id);
        jedis.setex(redisKey, PropertyUtil.getIntegerValue("JEDIS_STRING_TTL"), value);
    }

    /**
     * 从 HBase 中 获取数据
     * @param table Table 对象
     * @param rowKey rowKey
     * @return 返回一个 JSONObject 对象
     * @throws IOException table.get() 可能抛出的异常
     */
    protected JSONObject getValueFromHBase(Table table, String rowKey) throws IOException {
        JSONObject res = new JSONObject();
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell cell : cells) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            res.put(qualifier, value);
        }
        return res;
    }


    /**
     * 向 Hbase 中 写入一行数据 data
     * @param rowKey rowKey
     * @param sinkFamily 列族
     * @param data 写入的数据
     * @return 返回一个 Put 对象
     */
    public static Put createPut(String rowKey, String sinkFamily, JSONObject data) {
        if(rowKey == null || sinkFamily == null || data == null) {
            log.warn("DimOperateBaseFunction::createPut nullPointer warn!");
            return null;
        }
        byte[] family = Bytes.toBytes(sinkFamily);
        Put put = new Put(Bytes.toBytes(rowKey));
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

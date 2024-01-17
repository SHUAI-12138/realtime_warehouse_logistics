package com.atguigu.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.HBaseUtil;
import com.atguigu.realtime.util.PropertyUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSinkFunction extends DimOperateBaseFunction implements SinkFunction<Tuple2<JSONObject, TableProcess>> {

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        JSONObject data = value.f0;
        TableProcess tableProcess = value.f1;

        String sourceTable = tableProcess.getSourceTable();
        Table table = tableMap.get(sourceTable);
        if(table == null) {
            table = HBaseUtil.getTable(PropertyUtil.getStringValue("HBASE_NAMESPACE"), sourceTable);
            tableMap.put(sourceTable, table);
        }
        if(table == null) return;

        String sinkRowKey = tableProcess.getSinkRowKey();
        String rowKey = data.getString(sinkRowKey);
        String sinkFamily = tableProcess.getSinkFamily();
        String type = data.getString("type");

        if (type.equals("delete")) {
            table.delete(new Delete(Bytes.toBytes(rowKey)));
            jedis.del(getRedisKey(sourceTable, rowKey));
        } else {
            table.put(createPut(rowKey, sinkFamily, data));
            setStringToRedis(sourceTable, rowKey, data.toJSONString());
        }

    }
}

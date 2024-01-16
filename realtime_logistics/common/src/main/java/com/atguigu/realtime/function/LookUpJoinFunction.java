package com.atguigu.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.util.HBaseUtil;
import com.atguigu.realtime.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

@Slf4j
public abstract class LookUpJoinFunction<T> extends DimOperateBaseFunction implements MapFunction<T,T>
{
    //关联哪个维度(业务表)
    private final String dimTable;

    public abstract String getIdValue(T value);

    /**
     * 获取 要查询的表
     * @param dimTable 要查询的表名
     */
    public LookUpJoinFunction(String dimTable){
        this.dimTable = dimTable;
    }


    /**
     * 查询维度数据
     * @param value The input value.
     * @return 返回 将查询数据合并进 value 后的 value
     */
    @Override
    public T map(T value) throws IOException {

        JSONObject dimData = null;
        String k = getIdValue(value);
        /*
            从缓存中读取维度信息
                redis中查询value为String的值，如果key不存在，返回值为null
                redis中查询value为Set的值，如果key不存在，返回值不为null，返回空集合[]
         */
        String v = getStringFromRedis(dimTable,k );

        if (v == null){
            //如果缓存中读不到，访问hbase
            Table table = tableMap.get(dimTable);
            if (table == null){
                String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
                table = HBaseUtil.getTable(namespace, "dim_" + dimTable);
                tableMap.put(dimTable,table);
            }
            if(table == null) return null;
            dimData = getValueFromHBase(table, k);
            //把hbase读到的数据，写入到缓存，方便后续使用
            setStringToRedis(dimTable,k,dimData.toJSONString());
            // log.warn("从hbase查询....");
        }else {
            dimData = JSON.parseObject(v);
           //  log.warn("从redis查询....");
        }
        //读到维度数据后，把想要的字段添加到事实上
        extractDimData(value,dimData);

        return value;
    }

    /**
     * 将 读取到的维度数据添加进value中
     * @param value 最终的值
     * @param dimData 查询到的维度数据
     */
    protected abstract void extractDimData(T value, JSONObject dimData) ;
}
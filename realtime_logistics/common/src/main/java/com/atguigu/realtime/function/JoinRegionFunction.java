package com.atguigu.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.hbase.client.Table;

/**
 * base_region_info 比较特别， 需要不断关联自身从而获取城市和省份
 */
@Slf4j
public class JoinRegionFunction extends DimOperateBaseFunction implements MapFunction<JSONObject, JSONObject> {

    @Override
    public JSONObject map(JSONObject value) throws Exception {
        // 初始 region_id
        String regionId = value.getString("region_id");
        JSONObject data = null;
        // region_id=86 此时已经获取到省份 结束循环
        while(!regionId.equals("86")) {
            String redisData = getStringFromRedis("base_region_info", regionId);
            System.err.println(redisData);
            if(redisData == null) {
                Table table = tableMap.get("base_region_info");
                data = getValueFromHBase(table, regionId);
                setStringToRedis("base_region_info", regionId, data.toJSONString());
            } else {
                data = JSON.parseObject(redisData);
            }
            // 更新 region_id
            regionId = data.getString("parent_id");
            String name = data.getString("name");
            String dictCode = data.getString("dict_code");
            switch (dictCode){
                case "Province":
                    value.put("province", name);
                    break;
                case "City":
                    value.put("city", name);
                    break;
                default:
                    break;
            }
        }
        return value;
    }
}

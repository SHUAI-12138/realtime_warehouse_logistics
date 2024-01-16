package com.atguigu.realtime.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

public class AsyncRedisUtil
{
    //获取一个异步客户端
    public static RedisClient getAsyncRedisClient() {
        String url = "redis://%s:%s/%s";
        return RedisClient.create(String.format(url,
            PropertyUtil.getStringValue("JEDIS_POOL_HOST"),
            PropertyUtil.getStringValue("JEDIS_POOL_PORT"),
            PropertyUtil.getIntegerValue("JEDIS_DB_ID")
        ));
    }

    //使用客户端获取一个支持异步操作的连接
    public static StatefulRedisConnection<String, String> getAsyncRedisConnection(RedisClient client) {
        return client.connect();
    }

    //关闭异步客户端
    public static void closeAsyncClient(RedisClient client){
        if (client != null) {
            client.close();
        }
    }
    //关闭连接
    public static void closeAsyncConn( StatefulRedisConnection<String, String> conn){
        if (conn != null) {
            conn.close();
        }
    }
}
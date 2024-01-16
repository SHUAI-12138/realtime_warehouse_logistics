package com.atguigu.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil
{
    private static final JedisPool pool;

    static {

        //定制连接池的参数
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //池子的最大容量
        jedisPoolConfig.setMaxTotal(PropertyUtil.getIntegerValue("JEDIS_POOL_MAXTOTAL"));
        //池子中最小存活的连接数
        jedisPoolConfig.setMinIdle(PropertyUtil.getIntegerValue("JEDIS_POOL_MINIDLE"));
        //池子中最多存活的连接数
        jedisPoolConfig.setMaxIdle(PropertyUtil.getIntegerValue("JEDIS_POOL_MAXIDLE"));
        //阻塞时等待的最大时间，超过这个时间，依旧无法获取连接，就抛异常
        jedisPoolConfig.setMaxWaitMillis(PropertyUtil.getIntegerValue("JEDIS_POOL_MAXWAITMILLIS"));
        //客户端来借连接了，但是连接耗尽了，客户端要不要等一等(阻塞)
        jedisPoolConfig.setBlockWhenExhausted(true);
        //借连接时，先测试以下好使，再借
        jedisPoolConfig.setTestOnBorrow(true);
        //还连接时，先测试以下好使，还借
        jedisPoolConfig.setTestOnReturn(true);

        //创建一个连接池
         pool = new JedisPool(
                 jedisPoolConfig,
                 PropertyUtil.getStringValue("JEDIS_POOL_HOST"),
                 PropertyUtil.getIntegerValue("JEDIS_POOL_PORT")
         );

    }

    public static Jedis getRedisClient() {
        Jedis jedis = pool.getResource();
        jedis.select(PropertyUtil.getIntegerValue("JEDIS_DB_ID"));  // 选择1号库
        return jedis;
    }

    public static void closeRedisClient(Jedis redisClient) {
        if (redisClient != null) {
            // 如果客户端是通过 new jedis 出来的,则是关闭。如果客户端是通过连接池获取的,则是归还
            redisClient.close();
        }
    }

}
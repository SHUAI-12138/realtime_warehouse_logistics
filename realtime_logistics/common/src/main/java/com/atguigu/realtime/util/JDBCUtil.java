package com.atguigu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.List;


public class JDBCUtil {

    private static final DruidDataSource pool;

    // 初始化连接池
    static {
        pool = new DruidDataSource();
        pool.setUrl(PropertyUtil.getStringValue("MYSQL_URL"));
        pool.setUsername(PropertyUtil.getStringValue("MYSQL_USER"));
        pool.setPassword(PropertyUtil.getStringValue("MYSQL_PASSWORD"));

        pool.setInitialSize(PropertyUtil.getIntegerValue("POOL_INITIAL_SIZE")); // 初始化连接数
        pool.setMaxActive(PropertyUtil.getIntegerValue("POOL_MAX_ACTIVE")); // 最大连接数
        pool.setMinIdle(PropertyUtil.getIntegerValue("POOL_MIN_IDLE")); // 最小空闲连接数
        pool.setMaxWait(PropertyUtil.getIntegerValue("POOL_MAX_WAITTIME")); // 获取连接的最大等待
    }

    /**
     * 从连接池中获取连接
     * @return 返回连接
     * @throws SQLException 获取连接时可能抛出的异常
     */
    public static DruidPooledConnection getConnection() throws SQLException {
        return pool.getConnection();
    }

    /**
     * 将连接返回给连接池
     * @param connection 连接
     * @throws SQLException 关闭连接时可能抛出的异常
     */
    public static void closeConnection(DruidPooledConnection connection) throws SQLException {
        if(connection != null) {
            connection.close();
        }
    }

    /**
     * 将查询的结果放在List中返回，泛型需要传入Bean的类型
     * @param sql 查询语句
     * @param t Bean 的 class
     * @return 结果集， 存储在 List 中
     * @param <T> Bean 的 类型
     */
    public static <T> List<T> queryBeanList(String sql, Class<T> t) throws SQLException {
        // 获取连接
        DruidPooledConnection connection = getConnection();
        QueryRunner queryRunner = new QueryRunner();
        BeanListHandler<T> handler = new BeanListHandler<>(t, new BasicRowProcessor(new GenerousBeanProcessor()));
        // Connection conn, String sql, ResultSetHandler<T> rsh
        List<T> query = queryRunner.query(connection, sql, handler);
        // 关闭连接
        closeConnection(connection);
        return query;
    }
}

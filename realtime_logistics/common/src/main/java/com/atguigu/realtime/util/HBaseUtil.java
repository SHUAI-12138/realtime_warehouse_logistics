package com.atguigu.realtime.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseUtil {
    static public Connection connection;
    static {
        try {
            // 默认读取 hbase-site.xml 中的配置信息
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭客户端
     */
    public static void closeClient() {
        if(connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @return 返回 Hbase 管理员对象
     */
    public static Admin getAdmin() {
        Admin admin = null;
        try {
            admin =  connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }


    /**
     * 获取 TableName
     * @param namespace namespace
     * @param tableName table_name
     * @return TableName
     */
    public static TableName getTableName(String namespace, String tableName) {
        if(StringUtils.isAnyBlank(namespace, tableName)) {
            throw new RuntimeException("Namespace or tableName is illegal!");
        }
        return TableName.valueOf(namespace + ":" + tableName);
    }

    /**
     * 检查表是否已经存在
     * @param namespace namespace
     * @param tableName table_name
     * @return 是为 true， 否为 false
     * @throws IOException admin.tableExists() 可能会抛出异常
     */
    public static boolean checkTableExist(Admin admin, String namespace, String tableName) throws IOException {
        if(StringUtils.isAnyBlank(namespace, tableName)) {
            throw new RuntimeException("Namespace or tableName is illegal!");
        }
        return admin.tableExists(getTableName(namespace, tableName));
    }

    /**
     * 检查命名空间是否已经存在
     * @param namespace namespace
     * @return 存在为 true， 反之为 false
     */
    public static boolean checkNamespaceExist(Admin admin, String namespace) {
        if(StringUtils.isBlank(namespace)) {
            throw new RuntimeException("Namespace is illegal!");
        }
        // 如果没有该 namespace， admin.getNamespaceDescriptor(namespace) 会抛出 NamespaceNotFoundException
        // 没有捕获到异常 返回true
        // 捕获到 异常 返回 false
        try {
            admin.getNamespaceDescriptor(namespace);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 创建 namespace
     * @param namespace 命名空间名称
     * @throws IOException admin.createNamespace() 可能会抛出异常
     */
    public static void createNamespace(Admin admin,String namespace) throws IOException {
        if(StringUtils.isBlank(namespace)) {
            throw new RuntimeException("Namespace is illegal!");
        }
        // 如果 namespace 已经存在 直接返回
        if(checkNamespaceExist(admin, namespace)) return;
        admin.createNamespace(NamespaceDescriptor.create(namespace).build());
    }

    /**
     * 创建表
     * @param namespace namespace
     * @param tableName table_name
     * @param cfs 列族， 可变长参数
     */
    public static void createTable(Admin admin, String namespace, String tableName, String ... cfs) throws IOException {
        if(StringUtils.isAnyBlank(namespace, tableName)) {
            throw new RuntimeException("Namespace or tableName is illegal!");
        }
        if(cfs.length < 1) {
            throw new RuntimeException("Expect at least one column, but actually zero.");
        }
        // 创建命名空间
        createNamespace(admin, namespace);
        // 如果表已经存在，直接返回
        if(checkTableExist(admin, namespace, tableName)) return;
        // 不存在， 创建
        List<ColumnFamilyDescriptor> collect = Arrays.stream(cfs).map(ColumnFamilyDescriptorBuilder::of).collect(Collectors.toList());
        TableDescriptor tableDescriptor = TableDescriptorBuilder
                .newBuilder(getTableName(namespace, tableName))
                .setColumnFamilies(collect)
                .build();
        admin.createTable(tableDescriptor);
    }

    /**
     * 删除表
     * @param admin 管理员
     * @param namespace namespace
     * @param tableName table_name
     * @return 删除成功返回true，否则返回false
     */
    public static boolean dropTable(Admin admin, String namespace, String tableName) throws IOException {
        if(StringUtils.isAnyBlank(namespace, tableName)) {
            throw new RuntimeException("Namespace or tableName is illegal!");
        }
        if(!checkNamespaceExist(admin, namespace)) return false;
        if(!checkTableExist(admin, namespace, tableName)) return false;
        TableName table = getTableName(namespace, tableName);
        // 先禁用
        admin.disableTable(table);
        // 删除表
        admin.deleteTable(table);
        return true;
    }

    /**
     * 获取 Table 对象
     * @param namespace 命名空间
     * @param tableName 表名
     * @return 返回 该表的 Table 对象
     */
    public static Table getTable(String namespace, String tableName) {
        try {
            return connection.getTable(getTableName(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取 Put 对象， Table.put(Put put); 用于向 Hbase中添加数据
     * 只能插入一个cell
     * @param rowKey rowKey
     * @param cf 列族
     * @param cq 列名
     * @param value 列值
     * @return 返回一个 Put 对象
     */
    public static Put createPut(String rowKey, String cf, String cq, String value) {
        Put put = new Put(Bytes.toBytes(rowKey));
        return put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(value));
    }

    // 异步 读写 hbase 的 连接
    private static final AsyncConnection asyncConnection;

    static {
        try {
            //在方法的构造中，会读取hadoop的各种配置及hbase的各种配置
            asyncConnection = ConnectionFactory.createAsyncConnection().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAsyncConn() throws IOException {
        if (asyncConnection != null){
            asyncConnection.close();
        }
    }

    /**
     * 获取 异步 读写表的 AsyncTable 对象
     * @param namespace 命名空间
     * @param tableName  表名 sinkTable
     * @return 返回一个 异步操作表的表对象 AsyncTable
     */
    public static AsyncTable<AdvancedScanResultConsumer> getAsyncTable(String namespace,String tableName){
        return asyncConnection.getTable(getTableName(namespace, tableName));
    }


    public static void printResult(Result result) {
        Cell[] cells = result.rawCells();
        for(Cell cell : cells) {
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long timestamp = cell.getTimestamp();
            String type = cell.getType().toString();
            System.out.println(row + ":" + family + ", " + qualifier + ", " + value + ", " + timestamp + ", " + type);
        }
    }

}

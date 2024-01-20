package com.atguigu.dwd.transport;

import com.atguigu.realtime.base.BaseSqlApp;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.atguigu.realtime.constant.LogisticsConstant.*;

public class TaskDetailSql extends BaseSqlApp {
    public static void main(String[] args) {
        new TaskDetailSql()
            .start(
                15001,
                4,
                TOPIC_DWD_TRANSPORT_TASK_DETAIL
            );
    }
    
    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        createOdsDB(tableEnv, TOPIC_DWD_TRANSPORT_TASK_DETAIL);
        // 过滤transport_task的数据
        String transportTaskSql = " select " +
            "  data['id'] id, " +
            "  data['shift_id'] shift_id, " +
            "  data['line_id'] line_id, " +
            "  data['status'] status, " +
            "  data['order_num'] order_num, " +
            "  data['truck_id'] truck_id, " +
            "  data['start_org_id'] start_org_id, " +
            "  data['start_org_name'] start_org_name, " +
            "  data['actual_start_time'] actual_start_time, " +
            "  data['actual_end_time'] actual_end_time, " +
            "  data['actual_distance'] actual_distance, " +
            "  ts " +
            "  from ods_db " +
            "  where `database` = 'tms' " +
            "  and `table` = 'transport_task' " +
            "  and `type` = 'update' " +
            "  and `data`['order_num'] > 0" +
            "  and `data`['actual_end_time'] is not null";
        //tableEnv.executeSql(transportTaskSql).print();
        // 过滤transport_task_detail的数据
        String transportDetailSql = " select " +
            "  type, " +
            "  data['id'] id, " +
            "  data['order_id'] order_id, " +
            "  data['transport_task_id'] transport_task_id, " +
            "  data['create_time'] create_time, " +
            "  data['operate_time'] operate_time " +
            "  from ods_db " +
            "  where `database` = 'tms' " +
            "  and `table` = 'transport_task_detail' " +
            "  and `type` = 'update' ";
        //tableEnv.executeSql(transportDetailSql).print();
        tableEnv.createTemporaryView("tt", tableEnv.sqlQuery(transportTaskSql));
        tableEnv.createTemporaryView("td", tableEnv.sqlQuery(transportDetailSql));
        
        // 关联
        String joinSql = " select " +
            "  tt.id, " +
            "  shift_id, " +
            "  line_id, " +
            "  order_id, " +
            "  status, " +
            "  order_num, " +
            "  truck_id, " +
            "  start_org_id, " +
            "  start_org_name, " +
            "  actual_start_time, " +
            "  actual_end_time, " +
            "  actual_distance, " +
            "  ts " +
            "  from tt " +
            "  join td on tt.id = td.transport_task_id ";
        //tableEnv.executeSql(joinSql).print();
        
        // 创建dwd_transport_task_detail表
        String sinkSql = " create table " + TOPIC_DWD_TRANSPORT_TASK_DETAIL + "(" +
            "  id STRING , " +
            "  shift_id STRING , " +
            "  line_id STRING , " +
            "  order_id STRING , " +
            "  status STRING , " +
            "  order_num STRING , " +
            "  truck_id STRING , " +
            "  start_org_id STRING , " +
            "  start_org_name STRING , " +
            "  actual_start_time STRING , " +
            "  actual_end_time STRING , " +
            "  actual_distance STRING , " +
            "  ts BIGINT " +
            "  )" + SqlUtil.getKafkaSinkConfigSql(TOPIC_DWD_TRANSPORT_TASK_DETAIL);
        tableEnv.executeSql(sinkSql);
        
        // 写出到kafka
        tableEnv.executeSql(" insert into " + TOPIC_DWD_TRANSPORT_TASK_DETAIL + joinSql);
        //tableEnv.executeSql("insert into " + TOPIC_DWD_TRANSPORT_TASK_DETAIL + transportTaskSql);
    }
}

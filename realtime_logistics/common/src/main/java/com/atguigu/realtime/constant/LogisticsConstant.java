package com.atguigu.realtime.constant;

public interface LogisticsConstant
{
    long SEVEN_DAY_MS = 7 * 24 * 60 * 60 * 1000;
    int TWO_DAY_SECONDS = 2 * 24 * 60 * 60;
    
    // DWD
    String TOPIC_DWD_TRANSPORT_TASK_DETAIL = "dwd_transport_task_detail";
    String TOPIC_DWD_EXPRESS_SORTING = "dwd_express_sorting";
    String TOPIC_DWD_EXPRESS_DELIVER = "dwd_express_deliver";
    String TOPIC_DWD_EXPRESS_PICKUP = "dwd_express_pickup";
    String TOPIC_DWD_TRADE_ORDER_CARGO = "dwd_trade_order_cargo";
    String TOPIC_DWD_ORDER_STATUS_COUNT = "dwd_order_status_count";
    String TOPIC_DWD_TRADE_ORDER_ORG = "dwd_trade_order_org";
    String TOPIC_DWD_TRANSPORT_TRUCK_TASK = "dwd_transport_truck_task";
}
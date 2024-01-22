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

    // DWS
    String DWS_ORDER_STATUS_COUNT_VIEW_WINDOW = "logistics.dws_order_status_count_view_window";
    String DWS_TRANSPORT_TOTAL_COUNT_VIEW_WINDOW = "logistics.dws_transport_total_count_view_window";
    String DWS_TRADE_ORDER_CARGO_VIEW_WINDOW = "logistics.dws_trade_order_cargo_view_window";
    String DWS_TRADE_ORDER_CITY_VIEW_WINDOW = "logistics.dws_trade_order_city_view_window";
    String DWS_TRADE_ORDER_ORG_VIEW_WINDOW = "logistics.dws_trade_order_org_view_window";
    String DWS_TRANSPORT_CITY_ORG_VIEW_WINDOW = "logistics.dws_transport_city_org_view_window";
    String DWS_TRANSPORT_TRUCK_VIEW_WINDOW = "logistics.dws_transport_truck_view_window";
    String DWS_EXPRESS_DELIVER_VIEW_WINDOW = "logistics.dws_express_deliver_view_window";
    String DWS_EXPRESS_PICKUP_VIEW_WINDOW = "logistics.dws_express_pickup_view_window";
    String DWS_EXPRESS_SORTING_VIEW_WINDOW = "logistics.dws_express_sorting_view_window";
}
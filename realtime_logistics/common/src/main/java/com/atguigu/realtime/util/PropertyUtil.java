package com.atguigu.realtime.util;

import java.util.ResourceBundle;

public class PropertyUtil {

    static ResourceBundle config;

    static {
        config = ResourceBundle.getBundle("config");
    }

    /**
     * 从配置文件 config.properties 中获取 String 对象
     * @param name 配置名
     * @return 配置信息
     */
    public static String getStringValue(String name) {
        return config.getString(name);
    }

    /**
     * 从配置文件中 config.properties 获取 Integer 对象
     * @param name 配置名
     * @return 配置信息
     */
    public static Integer getIntegerValue(String name) {
        return Integer.parseInt(config.getString(name));
    }

    /**
     * 从配置文件中 config.properties 获取 boolean 值
     * @param name 配置名
     * @return 配置信息
     */
    public static boolean getBooleanValue(String name) {
        return config.getString(name).equals("true");
    }

}

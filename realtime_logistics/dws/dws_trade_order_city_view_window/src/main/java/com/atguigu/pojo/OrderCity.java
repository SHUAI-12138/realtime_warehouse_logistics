package com.atguigu.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCity {
    private String stt;
    private String edt;
    private String curDate;
    private String cityId;
    private String cityName;
    private String provinceId;
    private String provinceName;
    private Long orderCount = 1L;
    private Double totalAmount;

    @JSONField(serialize = false)
    private Long ts;

    public void sum(OrderCity other) {
        this.orderCount += other.getOrderCount();
        this.totalAmount += other.getTotalAmount();
    }

}

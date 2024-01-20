package com.atguigu.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderStatus {
    private String stt;
    private String edt;
    private String curDate;
    private Long tackCount = 0L;
    private Long sendCount = 0L;
    private Long tcCount = 0L;

    @JSONField(serialize = false)
    Long ts;

    public void sum(OrderStatus other) {
        this.tackCount += other.getTackCount();
        this.sendCount += other.getSendCount();
        this.tcCount += other.getTcCount();
    }
}

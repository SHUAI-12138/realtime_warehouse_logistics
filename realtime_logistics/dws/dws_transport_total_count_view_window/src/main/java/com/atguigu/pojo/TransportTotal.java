package com.atguigu.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransportTotal {
    private String stt;
    private String edt;
    private String curDate;
    private Long transCount = 1L;
    private Double transDistance;
    private Long transTime;

    @JSONField(serialize = false)
    private Long ts;

    public void sum(TransportTotal other) {
        this.transCount += other.getTransCount();
        this.transDistance += other.getTransDistance();
        this.transTime += other.getTransTime();
    }
}

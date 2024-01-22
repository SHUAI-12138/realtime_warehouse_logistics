package com.atguigu.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransportTruck {
    private String stt;
    private String edt;
    private String curDate;
    private String truckModeId;
    private String truckModeType;
    private String truckModeName;
    private Long transCount = 1L;
    private Double transDistance;
    private Long transTime;

    @JSONField(serialize = false)
    private Long ts;
    @JSONField(serialize = false)
    private String truckId;

    public void sum(TransportTruck other) {
        this.transDistance += other.getTransDistance();
        this.transCount += other.getTransCount();
        this.transTime += other.getTransTime();
    }
}

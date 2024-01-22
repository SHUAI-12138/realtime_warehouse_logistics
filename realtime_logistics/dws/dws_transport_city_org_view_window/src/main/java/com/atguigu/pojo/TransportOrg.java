package com.atguigu.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransportOrg {
    private String stt;
    private String edt;
    private String curDate;
    private String provinceId;
    private String provinceName;
    private String cityId;
    private String cityName;
    private String orgId;
    private String orgName;
    private Long transCount = 1L;
    private Double transDistance;
    private Long transTime;

    @JSONField(serialize = false)
    private Long ts;

    public void sum(TransportOrg other) {
        this.transCount += other.getTransCount();
        this.transDistance += other.getTransDistance();
        this.transTime += other.getTransTime();
    }
}

package com.atguigu.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExpressDeliver {
    private String stt;
    private String edt;
    private String curDate;
    private String provinceId;
    private String provinceName;
    private String cityId;
    private String cityName;
    private String orgId;
    private String orgName;
    private Long count = 1L;

    @JSONField(serialize = false)
    private Long ts;

    public void sum(ExpressDeliver other) {
        this.count += other.getCount();
    }
}

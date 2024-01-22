package com.atguigu.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderOrg {
    private String stt;
    private String edt;
    private String curDate;
    private String orgId;
    private String orgName;
    private Long count = 1L;
    private Double amount;

    @JSONField(serialize = false)
    private Long ts;

    public void sum(OrderOrg other) {
        this.count += other.getCount();
        this.amount += other.getAmount();
    }
}

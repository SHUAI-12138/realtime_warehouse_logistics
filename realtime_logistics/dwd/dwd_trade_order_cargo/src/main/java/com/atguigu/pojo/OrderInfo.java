package com.atguigu.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderInfo {
    private Double amount;
    private String id;
    private Integer cargoNum;
    private String senderCityId;
    private String senderProvinceId;
}

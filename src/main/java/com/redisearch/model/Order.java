package com.redisearch.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Order {
    private String schemeCode;
    private String stateName;
    private String internalOrdNo;
    private String dealerId;
    private String marketCode;
    private String ordSide;
    private String commodityCode;
    private String contractCode;
    private String auctionType;


}
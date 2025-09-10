package com.suhoi.mexcwebsocket.domain.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class L1 {
    private BigDecimal bid;
    private BigDecimal ask;
    private long ts;
}

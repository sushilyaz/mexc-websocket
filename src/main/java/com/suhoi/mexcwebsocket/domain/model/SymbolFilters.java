package com.suhoi.mexcwebsocket.domain.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@ToString
public class SymbolFilters {
    private BigDecimal tickSize;     // PRICE_FILTER.tickSize
    private BigDecimal stepSize;     // LOT_SIZE.stepSize
    private BigDecimal minQty;       // LOT_SIZE.minQty
    private BigDecimal minNotional;  // MIN_NOTIONAL.minNotional
    private Integer quotePrecision;  // точность котируемой
}

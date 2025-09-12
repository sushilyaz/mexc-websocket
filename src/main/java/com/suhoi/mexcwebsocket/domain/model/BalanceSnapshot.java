package com.suhoi.mexcwebsocket.domain.model;

import java.math.BigDecimal;

public record BalanceSnapshot(
        BigDecimal aUsdt, BigDecimal bUsdt,
        BigDecimal aBase, BigDecimal bBase,
        String base
) { }

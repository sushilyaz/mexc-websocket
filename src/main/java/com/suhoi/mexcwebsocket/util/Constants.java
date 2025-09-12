package com.suhoi.mexcwebsocket.util;

import com.suhoi.mexcwebsocket.domain.model.BalanceSnapshot;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Constants {
    public static final Map<Long, BalanceSnapshot> startBalances = new ConcurrentHashMap<>();

    // Значения по умолчанию
    public static final BigDecimal SPREAD_GUARD_DEFAULT = new BigDecimal("0.20");
    public static final int TICK_ABOVE_DEFAULT = 10;

    // ТЕКУЩИЕ (глобальные) значения — изменяемые командами бота
    public static volatile BigDecimal SPREAD_GUARD = SPREAD_GUARD_DEFAULT;
    public static volatile int TICK_ABOVE = TICK_ABOVE_DEFAULT;
}

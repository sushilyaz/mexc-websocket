package com.suhoi.mexcwebsocket.util;

import java.math.BigDecimal;

public class FormatHelpers {
    public static String fmt(BigDecimal x) {
        return x == null ? "null" : x.stripTrailingZeros().toPlainString();
    }
}

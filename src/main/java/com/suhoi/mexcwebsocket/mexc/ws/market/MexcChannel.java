package com.suhoi.mexcwebsocket.mexc.ws.market;

import java.util.Locale;
import java.util.Objects;

public final class MexcChannel {
    private MexcChannel() {}
    private static String up(String s) { return Objects.requireNonNull(s).toUpperCase(Locale.ROOT); }

    // Trades (raw deals aggregate 10/100ms)
    public static String deals(String symbol, int intervalMs) {
        return "spot@public.aggre.deals.v3.api.pb@" + intervalMs + "ms@" + up(symbol);
    }
    // BookTicker (best bid/ask) 10/100ms
    public static String bookTicker(String symbol, int intervalMs) {
        return "spot@public.aggre.bookTicker.v3.api.pb@" + intervalMs + "ms@" + up(symbol);
    }
    // Diff Depth (incremental) 10/100ms
    public static String diffDepth(String symbol, int intervalMs) {
        return "spot@public.increase.depths.v3.api.pb@" + intervalMs + "ms@" + up(symbol);
    }
    // Kline: Min1/5/15/30/60, Hour4/8, Day1, Week1, Month1
    public static String kline(String symbol, String interval) {
        return "spot@public.kline.v3.api.pb@" + up(symbol) + "@" + interval;
    }
    // FULL snapshot L2 (limit=5/10/20/50/100/...) — начальная и периодическая «база»
    public static String limitDepth(String symbol, int level) {
        return "spot@public.limit.depth.v3.api.pb@" + up(symbol) + "@" + level;
    }
    // агрегированный depth 10/100ms
    public static String aggreDepth(String symbol, int intervalMs) {
        return "spot@public.aggre.depth.v3.api.pb@" + intervalMs + "ms@" + up(symbol);
    }
}

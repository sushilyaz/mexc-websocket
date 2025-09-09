// mexc/ws/market/LocalOrderBook.java
package com.suhoi.mexcwebsocket.mexc.ws.market;

import java.math.BigDecimal;
import java.util.NavigableMap;
import java.util.TreeMap;

public class LocalOrderBook {
    private final String symbol;
    // bids — по убыванию цены, asks — по возрастанию
    private final NavigableMap<BigDecimal, BigDecimal> bids =
            new TreeMap<>((a, b) -> b.compareTo(a));
    private final NavigableMap<BigDecimal, BigDecimal> asks =
            new TreeMap<>();

    private volatile long lastUpdateTs;

    public LocalOrderBook(String symbol) {
        this.symbol = symbol.toUpperCase();
    }

    public String getSymbol() { return symbol; }

    public synchronized void reset() {
        bids.clear();
        asks.clear();
        lastUpdateTs = 0L;
    }

    public synchronized void setLastUpdateTs(long ts) { this.lastUpdateTs = ts; }
    public long getLastUpdateTs() { return lastUpdateTs; }

    public synchronized void putBid(BigDecimal px, BigDecimal qty) {
        if (qty.signum() == 0) bids.remove(px); else bids.put(px, qty);
    }

    public synchronized void putAsk(BigDecimal px, BigDecimal qty) {
        if (qty.signum() == 0) asks.remove(px); else asks.put(px, qty);
    }

    /** Снимок верхних N уровней (для логов/статуса). */
    public synchronized String topN(int n) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("OB[").append(symbol).append("] ");
        sb.append("Bids: ");
        bids.entrySet().stream().limit(n).forEach(e ->
                sb.append(e.getKey()).append("(").append(e.getValue()).append(") "));
        sb.append(" | Asks: ");
        asks.entrySet().stream().limit(n).forEach(e ->
                sb.append(e.getKey()).append("(").append(e.getValue()).append(") "));
        sb.append(" @").append(lastUpdateTs);
        return sb.toString();
    }

    // Можно добавить геттеры/копию для дальнейшей бизнес-логики
    public synchronized NavigableMap<BigDecimal, BigDecimal> copyBids() { return new TreeMap<>(bids); }
    public synchronized NavigableMap<BigDecimal, BigDecimal> copyAsks() { return new TreeMap<>(asks); }
}

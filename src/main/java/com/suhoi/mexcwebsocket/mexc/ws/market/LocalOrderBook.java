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
    private BigDecimal lastBestBidPx;
    private BigDecimal lastBestAskPx;
    private volatile long lastUpdateTs;

    public LocalOrderBook(String symbol) {
        this.symbol = symbol.toUpperCase();
    }

    public String getSymbol() { return symbol; }

    public synchronized void setLastUpdateTs(long ts) { this.lastUpdateTs = ts; }
    public long getLastUpdateTs() { return lastUpdateTs; }

    public synchronized void putBid(BigDecimal px, BigDecimal qty) {
        if (qty.signum() == 0) bids.remove(px); else bids.put(px, qty);
    }

    public synchronized void putAsk(BigDecimal px, BigDecimal qty) {
        if (qty.signum() == 0) asks.remove(px); else asks.put(px, qty);
    }
    public synchronized void applyBookTicker(BigDecimal bidPx, BigDecimal bidQty,
                                             BigDecimal askPx, BigDecimal askQty, long ts) {
        // убрать старые best, если цена изменилась
        if (lastBestBidPx != null && bidPx != null && bidPx.compareTo(lastBestBidPx) != 0) {
            bids.remove(lastBestBidPx);
        }
        if (lastBestAskPx != null && askPx != null && askPx.compareTo(lastBestAskPx) != 0) {
            asks.remove(lastBestAskPx);
        }

        if (bidPx != null) {
            putBid(bidPx, bidQty == null ? BigDecimal.ZERO : bidQty);
            lastBestBidPx = bidPx;
        }
        if (askPx != null) {
            putAsk(askPx, askQty == null ? BigDecimal.ZERO : askQty);
            lastBestAskPx = askPx;
        }
        lastUpdateTs = ts;
    }

    public synchronized void reset() {
        bids.clear();
        asks.clear();
        lastBestBidPx = null;
        lastBestAskPx = null;
        lastUpdateTs = 0L;
    }
    /** Снимок верхних N уровней (для логов/статуса). */
    // внутри LocalOrderBook
    public synchronized String topN(int n) {
        StringBuilder sb = new StringBuilder();

        var bestAsk = asks.isEmpty() ? null : asks.firstEntry();
        var bestBid = bids.isEmpty() ? null : bids.firstEntry();

        sb.append("ASKS (best->worst)\n");
        asks.entrySet().stream().limit(n).forEach(e ->
                sb.append("  ")
                        .append(e.getKey().stripTrailingZeros().toPlainString())
                        .append("  x ")
                        .append(e.getValue().stripTrailingZeros().toPlainString())
                        .append('\n')
        );

        sb.append("BIDS (best->worst)\n");
        bids.entrySet().stream().limit(n).forEach(e ->
                sb.append("  ")
                        .append(e.getKey().stripTrailingZeros().toPlainString())
                        .append("  x ")
                        .append(e.getValue().stripTrailingZeros().toPlainString())
                        .append('\n')
        );

        if (bestBid != null && bestAsk != null) {
            var spread = bestAsk.getKey().subtract(bestBid.getKey());
            sb.append("spread: ")
                    .append(spread.stripTrailingZeros().toPlainString())
                    .append(" (bestBid=")
                    .append(bestBid.getKey().stripTrailingZeros().toPlainString())
                    .append(", bestAsk=")
                    .append(bestAsk.getKey().stripTrailingZeros().toPlainString())
                    .append(")\n");
        }
        return sb.toString();
    }


    // Можно добавить геттеры/копию для дальнейшей бизнес-логики
    public synchronized NavigableMap<BigDecimal, BigDecimal> copyBids() { return new TreeMap<>(bids); }
    public synchronized NavigableMap<BigDecimal, BigDecimal> copyAsks() { return new TreeMap<>(asks); }
}

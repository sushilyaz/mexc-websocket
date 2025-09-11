// mexc/ws/market/OrderBookService.java
package com.suhoi.mexcwebsocket.mexc.ws.market;

import com.mxc.push.common.protobuf.PublicAggreBookTickerV3Api;
import com.mxc.push.common.protobuf.PublicAggreDepthsV3Api;
import com.mxc.push.common.protobuf.PublicIncreaseDepthsV3Api;
import com.mxc.push.common.protobuf.PublicLimitDepthsV3Api;
import com.suhoi.mexcwebsocket.domain.model.L1;
import com.suhoi.mexcwebsocket.mexc.rest.MexcRestClient;
import com.suhoi.mexcwebsocket.mexc.ws.core.MexcWsClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderBookService implements MexcWsClient.Listener {

    private final MexcWsClient ws;
    private final MexcRestClient rest;
    /** symbol -> локальный стакан */
    private final Map<String, LocalOrderBook> books = new ConcurrentHashMap<>();
    /** symbol -> подписан ли уже на каналы */
    private final Map<String, Boolean> subscribed = new ConcurrentHashMap<>();
    private final Map<String, Long> lastVersion = new ConcurrentHashMap<>();
    private static final int BOOK_CAP = 50;        // храним верхние 50 на сторону (или 10/20 — как нужно)
    private static final int PARTIAL_LEVELS = 20;  // partial для ресинка
    private static final long BOOK_STALE_MS = 2000; // сколько считаем стакан «свежим»

    // логи ордербука
    private final Map<String, AtomicBoolean> dirty = new ConcurrentHashMap<>();
    private final ScheduledExecutorService obLogScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ob-log");
        t.setDaemon(true);
        return t;
    });
    private final int LOG_TOP_N = 50;       // сколько уровней печатать
    private final long LOG_PERIOD_MS = 100000; // как часто печатать (если были изменения)

    @PostConstruct
    void init() {
        ws.addListener(this);
    }

    /** Начать отслеживать символ: snapshot + diff. Идемпотентно. */
    public void startTracking(String symbol) {
        final String s = symbol.toUpperCase();
        LocalOrderBook ob = books.computeIfAbsent(s, LocalOrderBook::new);

        // 1) REST-snapshot
        try {
            var snap = rest.getDepthSnapshot(s, 100);
            if (snap != null) {
                if (snap.getBids() != null) {
                    for (var it : snap.getBids()) {
                        ob.putBid(MexcRestClient.bd(it.get(0)), MexcRestClient.bd(it.get(1)));
                    }
                }
                if (snap.getAsks() != null) {
                    for (var it : snap.getAsks()) {
                        ob.putAsk(MexcRestClient.bd(it.get(0)), MexcRestClient.bd(it.get(1)));
                    }
                }
                ob.setLastUpdateTs(System.currentTimeMillis());
                log.info("🧊 REST SNAPSHOT {} (top 5)\n{}", s, ob.topN(5));
//                ensureLoggerStarted(s);
//                markDirty(s);
            }
        } catch (Exception e) {
            log.warn("REST snapshot failed for {}: {}", s, e.getMessage());
        }

        // 2) WS-подписки (идемпотентно)
        if (subscribed.putIfAbsent(s, Boolean.TRUE) == null) {
            ws.subscribe(MexcChannel.aggreDepth(s, 100));            // инкременты
            ws.subscribe(MexcChannel.limitDepth(s, PARTIAL_LEVELS)); // топ-N снимки (ресинк/подстраховка)
            ws.subscribe(MexcChannel.bookTicker(s, 100));            // L1 для сигналов
            log.info("📡 L2 tracking started for {}", s);
        }
    }

    /** Снимок топ-N уровней для отладки/статуса. */
    public String debugTopN(String symbol, int n) {
        LocalOrderBook ob = books.get(symbol.toUpperCase());
        return (ob == null) ? "No book for " + symbol : ob.topN(n);
    }

    /* ===== MexcWsClient.Listener: market-depth события ===== */

    @Override
    public void onBookTicker(String symbol, PublicAggreBookTickerV3Api bt, long ts) {
        final String s = symbol.toUpperCase();
        LocalOrderBook ob = books.get(s);
        if (ob == null) return;

        try {
            ob.applyBookTicker(
                    new BigDecimal(bt.getBidPrice()), new BigDecimal(bt.getBidQuantity()),
                    new BigDecimal(bt.getAskPrice()), new BigDecimal(bt.getAskQuantity()),
                    ts
            );
            ensureLoggerStarted(s);
            markDirty(s);
        } catch (Exception e) {
            log.debug("BookTicker parse error for {}: {}", s, e.toString());
        }
    }
    public NavigableMap<BigDecimal, BigDecimal> asksSnapshot(String symbol) {
        LocalOrderBook ob = books.get(symbol.toUpperCase());
        return (ob == null) ? new TreeMap<>() : ob.copyAsks();
    }
    public NavigableMap<BigDecimal, BigDecimal> bidsSnapshot(String symbol) {
        LocalOrderBook ob = books.get(symbol.toUpperCase());
        return (ob == null) ? new TreeMap<>((a, b)->b.compareTo(a)) : ob.copyBids();
    }

    @Override
    public void onAggreDepth(String symbol, PublicAggreDepthsV3Api d, long ts) {
        final String s = symbol.toUpperCase();
        var ob = books.get(s);
        if (ob == null) return;

        Long from = parseLongSafe(d.getFromVersion());
        Long to   = parseLongSafe(d.getToVersion());
        Long prev = lastVersion.get(s);

        if (prev != null && from != null && !from.equals(prev) && !from.equals(prev + 1)) {
            log.warn("❗Gap on aggre.depth {} prev={} from={} to={} → resync", s, prev, from, to);
            resyncFromRest(s);
            return;
        }

        for (int i = 0; i < d.getBidsCount(); i++) {
            var lvl = d.getBids(i);
            ob.putBid(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
        }
        for (int i = 0; i < d.getAsksCount(); i++) {
            var lvl = d.getAsks(i);
            ob.putAsk(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
        }

        if (to != null) lastVersion.put(s, to);
        else if (from != null) lastVersion.put(s, from);

        ob.setLastUpdateTs(ts);
        ob.trim(BOOK_CAP);
        markDirty(s);
        ensureLoggerStarted(s);
    }

    @Override
    public void onLimitDepth(String symbol, PublicLimitDepthsV3Api depth, long ts) {
        final String s = symbol.toUpperCase();
        var ob = books.get(s);
        if (ob == null) return;

        ob.reset();
        for (int i = 0; i < depth.getBidsCount(); i++) {
            var lvl = depth.getBids(i);
            ob.putBid(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
        }
        for (int i = 0; i < depth.getAsksCount(); i++) {
            var lvl = depth.getAsks(i);
            ob.putAsk(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
        }
        Long v = parseLongSafe(depth.getVersion());
        if (v != null) lastVersion.put(s, v);

        ob.setLastUpdateTs(ts);
        ob.trim(BOOK_CAP);
        markDirty(s);
    }


    @Override
    public void onDepthInc(String symbol, PublicIncreaseDepthsV3Api inc, long ts) {
        final String s = symbol.toUpperCase();
        var ob = books.get(s);
        if (ob == null) return;

        Long v = parseLongSafe(inc.getVersion()); // если этого поля реально нет — см. ниже
        Long prev = lastVersion.get(s);

        if (v != null && prev != null && !v.equals(prev + 1)) {
            log.warn("❗Gap on increase.depth {} prev={} v={} → resync", s, prev, v);
            resyncFromRest(s);
            return;
        }

        for (int i = 0; i < inc.getBidsCount(); i++) {
            var lvl = inc.getBids(i);
            ob.putBid(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
        }
        for (int i = 0; i < inc.getAsksCount(); i++) {
            var lvl = inc.getAsks(i);
            ob.putAsk(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
        }

        if (v != null) lastVersion.put(s, v); // если v нет — не трогаем lastVersion, полагаемся на aggre.depth/partial
        ob.setLastUpdateTs(ts);
        ob.trim(BOOK_CAP);
        markDirty(s);
        ensureLoggerStarted(s);
    }

    private static Long parseLongSafe(String v) {
        try { return (v == null || v.isEmpty()) ? null : Long.parseLong(v); }
        catch (Exception e) { return null; }
    }


    private void markDirty(String symbol) {
        dirty.computeIfAbsent(symbol, k -> new AtomicBoolean()).set(true);
    }

    private void ensureLoggerStarted(String symbol) {
        // запускать один раз на символ
        dirty.computeIfAbsent(symbol, k -> {
            // впервые увидели символ — стартуем периодический логгер
            obLogScheduler.scheduleAtFixedRate(() -> {
                try {
                    AtomicBoolean flag = dirty.get(symbol);
                    if (flag != null && flag.compareAndSet(true, false)) {
                        LocalOrderBook ob = books.get(symbol);
                        if (ob != null) {
                            log.info("📊 {} L2 (top {})\n{}", symbol, LOG_TOP_N, ob.topN(LOG_TOP_N));
                        }
                    }
                } catch (Throwable ignore) {}
            }, LOG_PERIOD_MS, LOG_PERIOD_MS, TimeUnit.MILLISECONDS);
            return new AtomicBoolean(false);
        });
    }
    private void resyncFromRest(String s) {
        try {
            var snap = rest.getDepthSnapshot(s, 100);
            LocalOrderBook ob = books.get(s);
            if (snap != null && ob != null) {
                ob.reset();
                if (snap.getBids() != null)
                    for (var it : snap.getBids()) ob.putBid(MexcRestClient.bd(it.get(0)), MexcRestClient.bd(it.get(1)));
                if (snap.getAsks() != null)
                    for (var it : snap.getAsks()) ob.putAsk(MexcRestClient.bd(it.get(0)), MexcRestClient.bd(it.get(1)));
                ob.trim(BOOK_CAP);
                ob.setLastUpdateTs(System.currentTimeMillis());
                markDirty(s);
                log.info("🔁 REST resync {}", s);
            }
        } catch (Exception e) {
            log.warn("REST resync failed for {}: {}", s, e.getMessage());
        }
    }

    /** Снимок L1 для symbol (или null, если стакана нет). */
    public L1 getSnapshotL1(String symbol) {
        LocalOrderBook ob = books.get(symbol.toUpperCase());
        return (ob == null) ? null : ob.getSnapshotL1();
    }

    /** Лучшая ask-цена или null. */
    public BigDecimal bestAsk(String symbol) {
        LocalOrderBook ob = books.get(symbol.toUpperCase());
        return (ob == null) ? null : ob.bestAskPx();
    }

    /** Лучшая bid-цена или null. */
    public BigDecimal bestBid(String symbol) {
        LocalOrderBook ob = books.get(symbol.toUpperCase());
        return (ob == null) ? null : ob.bestBidPx();
    }

    public boolean isFresh(String symbol) {
        LocalOrderBook ob = books.get(symbol.toUpperCase());
        if (ob == null) return false;
        long age = System.currentTimeMillis() - ob.getLastUpdateTs();
        return age >= 0 && age <= BOOK_STALE_MS;
    }
}

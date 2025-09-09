// mexc/ws/market/OrderBookService.java
package com.suhoi.mexcwebsocket.mexc.ws.market;

import com.mxc.push.common.protobuf.PublicAggreBookTickerV3Api;
import com.mxc.push.common.protobuf.PublicAggreDepthsV3Api;
import com.mxc.push.common.protobuf.PublicIncreaseDepthsV3Api;
import com.mxc.push.common.protobuf.PublicLimitDepthsV3Api;
import com.suhoi.mexcwebsocket.mexc.rest.MexcRestClient;
import com.suhoi.mexcwebsocket.mexc.ws.core.MexcWsClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.Map;
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


    // логи ордербука
    private final Map<String, AtomicBoolean> dirty = new ConcurrentHashMap<>();
    private final ScheduledExecutorService obLogScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ob-log");
        t.setDaemon(true);
        return t;
    });
    private final int LOG_TOP_N = 50;       // сколько уровней печатать
    private final long LOG_PERIOD_MS = 1000; // как часто печатать (если были изменения)

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
                ob.reset();
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
                ensureLoggerStarted(s);
                markDirty(s);
            }
        } catch (Exception e) {
            log.warn("REST snapshot failed for {}: {}", s, e.getMessage());
        }

        // 2) WS-подписки (идемпотентно)
        if (subscribed.putIfAbsent(s, Boolean.TRUE) == null) {
            ws.subscribe(MexcChannel.aggreDepth(s, 100));     // ← используем это
            ws.subscribe(MexcChannel.bookTicker(s, 100));     // топ для надёжности/быстроты
//            ws.subscribe(MexcChannel.diffDepth(s, 100)); // может быть Blocked — ок, fallback на тикер
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

    @Override
    public void onAggreDepth(String symbol, PublicAggreDepthsV3Api depth, long ts) {
        final String s = symbol.toUpperCase();
        LocalOrderBook ob = books.get(s);
        if (ob == null) return;

        try {
            ob.reset();
            for (int i = 0; i < depth.getBidsCount(); i++) {
                var lvl = depth.getBids(i);
                ob.putBid(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
            }
            for (int i = 0; i < depth.getAsksCount(); i++) {
                var lvl = depth.getAsks(i);
                ob.putAsk(new BigDecimal(lvl.getPrice()), new BigDecimal(lvl.getQuantity()));
            }
            ob.setLastUpdateTs(ts);
            markDirty(s);            // триггер логгера
            ensureLoggerStarted(s);
//             log.debug("🔁 AGGRE {} {}", s, ob.topN(5)); // если хочешь видеть rebuild
        } catch (Exception e) {
            log.warn("AggreDepth parse error for {}: {}", s, e.toString());
        }
    }
    @Override
    public void onLimitDepth(String symbol, PublicLimitDepthsV3Api depth, long ts) {
        final String s = symbol.toUpperCase();
        LocalOrderBook ob = books.get(s);
        if (ob == null) return; // не запрашивали этот символ

        // С нуля строим стакан
        ob.reset();
        try {
            // предполагаем структуру: lists asks/bids с полями price/quantity (string)
            for (int i = 0; i < depth.getBidsCount(); i++) {
                var lvl = depth.getBids(i);
                BigDecimal px  = new BigDecimal(lvl.getPrice());
                BigDecimal qty = new BigDecimal(lvl.getQuantity());
                ob.putBid(px, qty);
            }
            for (int i = 0; i < depth.getAsksCount(); i++) {
                var lvl = depth.getAsks(i);
                BigDecimal px  = new BigDecimal(lvl.getPrice());
                BigDecimal qty = new BigDecimal(lvl.getQuantity());
                ob.putAsk(px, qty);
            }
            ob.setLastUpdateTs(ts);
            log.debug("🔄 SNAPSHOT {} {}", s, ob.topN(5));
            ensureLoggerStarted(symbol);
            markDirty(symbol);
        } catch (Exception e) {
            log.warn("Snapshot parse error for {}: {}", s, e.toString());
        }
    }

    @Override
    public void onDepthInc(String symbol, PublicIncreaseDepthsV3Api inc, long ts) {
        final String s = symbol.toUpperCase();
        LocalOrderBook ob = books.get(s);
        if (ob == null) return; // не запрашивали этот символ

        try {
            // предполагаем такие же поля price/quantity (string) в обновлениях
            for (int i = 0; i < inc.getBidsCount(); i++) {
                var lvl = inc.getBids(i);
                BigDecimal px  = new BigDecimal(lvl.getPrice());
                BigDecimal qty = new BigDecimal(lvl.getQuantity());
                ob.putBid(px, qty); // qty=0 => удалим уровень
            }
            for (int i = 0; i < inc.getAsksCount(); i++) {
                var lvl = inc.getAsks(i);
                BigDecimal px  = new BigDecimal(lvl.getPrice());
                BigDecimal qty = new BigDecimal(lvl.getQuantity());
                ob.putAsk(px, qty);
            }
            ob.setLastUpdateTs(ts);
            // 👇 временно, чтобы увидеть, что диффы льются
            log.info("Δ {} bids={} asks={}", s, inc.getBidsCount(), inc.getAsksCount());

            ensureLoggerStarted(s);
            markDirty(s);
        } catch (Exception e) {
            log.warn("DepthInc parse error for {}: {}", s, e.toString());
        }
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

}

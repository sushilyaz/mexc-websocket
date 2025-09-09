// mexc/ws/market/OrderBookService.java
package com.suhoi.mexcwebsocket.mexc.ws.market;

import com.mxc.push.common.protobuf.PublicIncreaseDepthsV3Api;
import com.mxc.push.common.protobuf.PublicLimitDepthsV3Api;
import com.suhoi.mexcwebsocket.mexc.ws.core.MexcWsClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderBookService implements MexcWsClient.Listener {

    private final MexcWsClient ws;

    /** symbol -> локальный стакан */
    private final Map<String, LocalOrderBook> books = new ConcurrentHashMap<>();
    /** symbol -> подписан ли уже на каналы */
    private final Map<String, Boolean> subscribed = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        ws.addListener(this);
    }

    /** Начать отслеживать символ: snapshot + diff. Идемпотентно. */
    public void startTracking(String symbol) {
        final String s = symbol.toUpperCase();
        books.computeIfAbsent(s, LocalOrderBook::new);

        // Подпишемся один раз
        if (subscribed.putIfAbsent(s, Boolean.TRUE) == null) {
            // 1) Сразу запросим «полный» снимок с разумным лимитом (например 100)
            ws.subscribe(MexcChannel.limitDepth(s, 100));
            // 2) И дельты с периодом 100ms
            ws.subscribe(MexcChannel.diffDepth(s, 100));
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
            // можно не шуметь в логи на каждом тике; оставлю debug по запросу
            // log.debug("Δ {} {}", s, ob.topN(2));
        } catch (Exception e) {
            log.warn("DepthInc parse error for {}: {}", s, e.toString());
        }
    }
}

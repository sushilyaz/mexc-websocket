package com.suhoi.mexcwebsocket.mexc.ws.market;

import com.suhoi.mexcwebsocket.mexc.ws.core.MexcWsClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

// ТВОИ protobuf-типы:
import com.mxc.push.common.protobuf.PublicAggreBookTickerV3Api;
import com.mxc.push.common.protobuf.PublicAggreDealsV3Api;

@Slf4j
@Service
@ConditionalOnBean(MexcWsClient.class)
@RequiredArgsConstructor
public class DemoSubscriberService implements MexcWsClient.Listener {

    private final MexcWsClient client;

    @PostConstruct
    void init() {
        client.addListener(this);
        client.subscribe(
                MexcChannel.bookTicker("BTCUSDT", 100),
                MexcChannel.deals("BTCUSDT", 100)
        );
        log.info("▶️ DemoSubscriber subscribed (BTCUSDT bookTicker/deals 100ms)");
    }

    @PreDestroy
    void shutdown() {
        client.removeListener(this);
    }

    // ——— добавили базовые логи событий канала ———
    @Override public void onOpen() { log.info("✅ WS OPEN"); }
    @Override public void onAck(String json) { log.info("✅ ACK {}", json); }
    @Override public void onPong() { log.info("🏓 PONG"); }
    @Override public void onError(Throwable t) { log.error("💥 WS ERROR", t); }
    @Override public void onClosed(int code, String reason) { log.info("🔒 WS CLOSED {} {}", code, reason); }

    @Override
    public void onBookTicker(String symbol, PublicAggreBookTickerV3Api bt, long ts) {
        // Если в твоём .proto поля названы нижним кейсом (bidprice/askprice),
        // замени на getBidprice()/getAskprice()
//        log.info("📈 {} bid={}({}) ask={}({}) @{}", symbol,
//                bt.getBidPrice(), bt.getBidQuantity(), bt.getAskPrice(), bt.getAskQuantity(), ts);
    }

    @Override
    public void onDeals(String symbol, PublicAggreDealsV3Api deals, long ts) {
        if (deals.getDealsCount() > 0) {
            var d = deals.getDeals(0); // НЕ getDealsList(0)!
            // Если поле называется tradetype — используй getTradetype()
//            log.debug("💱 {} price={} qty={} type={} time={}",
//                    symbol, d.getPrice(), d.getQuantity(), d.getTradeType(), d.getTime());
        }
    }
}

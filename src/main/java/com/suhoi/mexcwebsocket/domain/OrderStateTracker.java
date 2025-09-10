package com.suhoi.mexcwebsocket.domain;
import com.suhoi.mexcwebsocket.domain.events.OrderEvent;
import com.suhoi.mexcwebsocket.infra.OrderEventBus;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
/** Держит текущее состояние ордеров и ретранслирует события в шину. */
public class OrderStateTracker {

    public static final class State {
        public final String symbol, clientId;
        public String status = "NEW";                    // NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED
        public BigDecimal cumQty = BigDecimal.ZERO;
        public BigDecimal cumQuote = BigDecimal.ZERO;
        public BigDecimal avgPrice = BigDecimal.ZERO;

        public State(String symbol, String clientId) { this.symbol = symbol; this.clientId = clientId; }
    }

    private final Map<String, State> byClient = new ConcurrentHashMap<>();
    private final OrderEventBus bus;

    public OrderStateTracker(OrderEventBus bus) { this.bus = bus; }

    /** Обработчик входящих (WS-)событий. */
    public void onEvent(OrderEvent e) {
        State s = byClient.computeIfAbsent(e.clientId(), k -> new State(e.symbol(), e.clientId()));
        if (e instanceof OrderEvent.OrderAccepted a) {
            bus.publish(a);
        } else if (e instanceof OrderEvent.OrderPartiallyFilled p) {
            s.status = "PARTIALLY_FILLED";
            s.cumQty = p.cumulativeQty(); s.cumQuote = p.cumulativeQuote(); s.avgPrice = p.avgPrice();
            bus.publish(p);
        } else if (e instanceof OrderEvent.OrderFilled f) {
            s.status = "FILLED";
            s.cumQty = f.cumulativeQty(); s.cumQuote = f.cumulativeQuote(); s.avgPrice = f.avgPrice();
            bus.publish(f);
        } else if (e instanceof OrderEvent.OrderCanceled c) {
            s.status = "CANCELED";
            s.cumQty = c.cumulativeQty(); s.cumQuote = c.cumulativeQuote(); s.avgPrice = c.avgPrice();
            bus.publish(c);
        } else if (e instanceof OrderEvent.OrderRejected r) {
            s.status = "REJECTED";
            bus.publish(r);
        } else if (e instanceof OrderEvent.OrderTrade t) {
            bus.publish(t);
        }
    }

    public State snapshot(String clientId) { return byClient.get(clientId); }
}

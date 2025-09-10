package com.suhoi.mexcwebsocket.infra;


import com.suhoi.mexcwebsocket.domain.events.OrderEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

@Component
public class OrderEventBus {
    public interface Subscription extends AutoCloseable { @Override void close(); }
    public interface Listener { void onEvent(OrderEvent e); }

    private final Map<Listener, Predicate<OrderEvent>> subs = new ConcurrentHashMap<>();

    public Subscription subscribe(Listener l, Predicate<OrderEvent> filter) {
        Objects.requireNonNull(l);
        subs.put(l, filter == null ? e -> true : filter);
        return () -> subs.remove(l);
    }

    public void publish(OrderEvent e) {
        subs.forEach((listener, pred) -> {
            try { if (pred.test(e)) listener.onEvent(e); } catch (Throwable ignore) {}
        });
    }

    // хелперы-фильтры
    public static Predicate<OrderEvent> byClientId(String clientId) { return e -> clientId.equals(e.clientId()); }
    public static Predicate<OrderEvent> any() { return e -> true; }
}

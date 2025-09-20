package com.suhoi.mexcwebsocket.domain;

import com.suhoi.mexcwebsocket.domain.events.OrderEvent;
import com.suhoi.mexcwebsocket.infra.OrderEventBus;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.math.BigDecimal.ZERO;

/** Держит текущее состояние ордеров, ретранслирует события в шину И УМЕЕТ ЖДАТЬ fill по clientId. */
@Component
public class OrderStateTracker {

    /** Снимок состояния для ожиданий. */
    public static final class FillSnapshot {
        public final String status;       // NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED / PARTIALLY_CANCELED?
        public final BigDecimal cumQty;   // суммарно исполнено (в базовой валюте)
        public final BigDecimal cumQuote; // суммарно исполнено (в котируемой)
        public final BigDecimal avgPrice;

        public FillSnapshot(String status, BigDecimal cumQty, BigDecimal cumQuote, BigDecimal avgPrice) {
            this.status = status;
            this.cumQty = cumQty;
            this.cumQuote = cumQuote;
            this.avgPrice = avgPrice;
        }

        public boolean isFinal() {
            return "FILLED".equals(status) || "CANCELED".equals(status) || "REJECTED".equals(status) || "PARTIALLY_CANCELED".equals(status);
        }
    }

    public static final class State {
        public final String symbol, clientId;
        public String status = "NEW";                    // NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED
        public BigDecimal cumQty = ZERO;
        public BigDecimal cumQuote = ZERO;
        public BigDecimal avgPrice = ZERO;

        // ожидающие «подписчики» на достижение порога filled
        final CopyOnWriteArrayList<Waiter> waiters = new CopyOnWriteArrayList<>();

        public State(String symbol, String clientId) { this.symbol = symbol; this.clientId = clientId; }

        FillSnapshot snapshot() { return new FillSnapshot(status, cumQty, cumQuote, avgPrice); }
    }

    private static final class Waiter {
        final BigDecimal target; // сколько ждём cumQty
        final CompletableFuture<FillSnapshot> fut = new CompletableFuture<>();
        ScheduledFuture<?> timeoutTask;
        Waiter(BigDecimal target) { this.target = target; }
    }

    private final Map<String, State> byClient = new ConcurrentHashMap<>();
    private final OrderEventBus bus;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "order-state-tracker-timer");
        t.setDaemon(true); return t;
    });

    public OrderStateTracker(OrderEventBus bus) { this.bus = bus; }

    /** Явно «подготовить» пустое состояние сразу после размещения ордера (до прихода первого WS-события). */
    public void ensure(String clientId, String symbol) {
        byClient.computeIfAbsent(clientId, k -> new State(symbol, clientId));
    }

    /** Снимок текущего состояния (может быть null). */
    public State snapshot(String clientId) { return byClient.get(clientId); }

    /** Удобный one-shot: дождаться cumQty >= target ИЛИ финального статуса, либо вернёт текущее на timeout. */
    public FillSnapshot awaitAtLeast(String clientId, BigDecimal target, Duration timeout) {
        Objects.requireNonNull(clientId, "clientId");
        if (target == null) target = ZERO;
        final State s = byClient.computeIfAbsent(clientId, k -> new State(null, clientId));

        // быстрый путь — прямо сейчас уже достаточно?
        if (s.cumQty.compareTo(target) >= 0 || isFinal(s.status)) {
            return s.snapshot();
        }

        final Waiter w = new Waiter(target);
        s.waiters.add(w);
        w.timeoutTask = timer.schedule(() -> w.fut.complete(s.snapshot()), timeout.toMillis(), TimeUnit.MILLISECONDS);

        try {
            return w.fut.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return s.snapshot();
        } catch (ExecutionException ee) {
            return s.snapshot();
        } finally {
            if (w.timeoutTask != null) w.timeoutTask.cancel(false);
            s.waiters.remove(w);
        }
    }

    private static boolean isFinal(String st) {
        return st != null && ("FILLED".equals(st) || "CANCELED".equals(st) || "REJECTED".equals(st) || "PARTIALLY_CANCELED".equals(st));
    }

    /** Обработчик входящих (WS-)событий. */
    public void onEvent(OrderEvent e) {
        State s = byClient.computeIfAbsent(e.clientId(), k -> new State(e.symbol(), e.clientId()));

        if (e instanceof OrderEvent.OrderAccepted a) {
            s.status = "NEW";
            // qty/price из accepted нам для cumQty не важны
            bus.publish(a);
        } else if (e instanceof OrderEvent.OrderPartiallyFilled p) {
            s.status = "PARTIALLY_FILLED";
            s.cumQty = nz(p.cumulativeQty()); s.cumQuote = nz(p.cumulativeQuote()); s.avgPrice = nz(p.avgPrice());
            bus.publish(p);
        } else if (e instanceof OrderEvent.OrderFilled f) {
            s.status = "FILLED";
            s.cumQty = nz(f.cumulativeQty()); s.cumQuote = nz(f.cumulativeQuote()); s.avgPrice = nz(f.avgPrice());
            bus.publish(f);
        } else if (e instanceof OrderEvent.OrderCanceled c) {
            // PARTIALLY_CANCELED тоже сюда (как у тебя в UserStreamRegistry)
            s.status = "CANCELED";
            s.cumQty = nz(c.cumulativeQty()); s.cumQuote = nz(c.cumulativeQuote()); s.avgPrice = nz(c.avgPrice());
            bus.publish(c);
        } else if (e instanceof OrderEvent.OrderRejected r) {
            s.status = "REJECTED";
            bus.publish(r);
        } else if (e instanceof OrderEvent.OrderTrade t) {
            // сделки могут приходить чаще «orders» — суммировать по ним не обязательно,
            // но пусть будет: если cumQty не приходит в orders, будем наращивать по deals
            s.cumQty = s.cumQty.add(nz(t.qty()));
            s.cumQuote = s.cumQuote.add(nz(t.amount()));
            bus.publish(t);
        }

        // разбудить ожидающих
        notifyWaiters(s);
    }

    private static BigDecimal nz(BigDecimal x) { return x == null ? ZERO : x; }

    private void notifyWaiters(State s) {
        if (s.waiters.isEmpty()) return;
        List<Waiter> ready = new ArrayList<>();
        for (Waiter w : s.waiters) {
            if (s.cumQty.compareTo(w.target) >= 0 || isFinal(s.status)) ready.add(w);
        }
        if (!ready.isEmpty()) {
            FillSnapshot snap = s.snapshot();
            ready.forEach(w -> w.fut.complete(snap));
        }
    }
}

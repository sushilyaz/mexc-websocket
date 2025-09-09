package com.suhoi.mexcwebsocket.mexc.ws.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mxc.push.common.protobuf.*;
import com.suhoi.mexcwebsocket.config.MexcWsProps;
import lombok.extern.slf4j.Slf4j;

// ==== ТВОИ сгенерированные protobuf-классы ====
// ==============================================

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public class MexcWsClient implements WebSocket.Listener {

    // ---- Минимальный Listener API ----
    public interface Listener {
        default void onOpen() {}
        default void onAck(String json) {}
        default void onPong() {}
        default void onDeals(String symbol, PublicAggreDealsV3Api deals, long sendTime) {}
        default void onDepthInc(String symbol, PublicIncreaseDepthsV3Api inc, long sendTime) {}
        default void onLimitDepth(String symbol, PublicLimitDepthsV3Api depth, long sendTime) {}
        default void onBookTicker(String symbol, PublicAggreBookTickerV3Api bt, long sendTime) {}
        default void onMiniTicker(String symbol, PublicMiniTickerV3Api mt, long sendTime) {}
        default void onMiniTickers(PublicMiniTickersV3Api mts, long sendTime) {}
        default void onKline(String symbol, PublicSpotKlineV3Api kline, long createTime) {}
        default void onError(Throwable t) {}
        default void onClosed(int status, String reason) {}
        // --- новые для user data streams ---
        default void onPrivateAccount(PrivateAccountV3Api acc, long sendTime) {}
        default void onPrivateDeals(String symbol, PrivateDealsV3Api deals, long sendTime) {}
        default void onPrivateOrders(String symbol, PrivateOrdersV3Api orders, long sendTime) {}
    }

    private final MexcWsProps props;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "mexc-ws");
        t.setDaemon(true);
        return t;
    });

    private final Set<String> subs = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);
    private volatile WebSocket ws;

    public MexcWsClient(MexcWsProps props) {
        this.props = props;
    }

    public void addListener(Listener l) { listeners.add(l); }
    public void removeListener(Listener l) { listeners.remove(l); }

    // ---- lifecycle ----
    public synchronized void connect() {
        if (!props.isEnabled()) {
            log.info("MEXC WS is disabled by property.");
            return;
        }
        URI uri = URI.create(props.getEndpoint());
        log.info("🔌 Connect {}", uri);
        http.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(uri, this)
                .whenComplete((sock, err) -> {
                    if (err != null) {
                        log.warn("❌ Connect failed: {}", err.toString());
                        scheduleReconnect(1);
                    } else {
                        this.ws = sock;
                        sock.request(1);
                        fire(Listener::onOpen);

                        // ПИНГ и ротация
                        scheduler.scheduleAtFixedRate(this::safePing,
                                props.getPingPeriodSec(), props.getPingPeriodSec(), TimeUnit.SECONDS);
                        scheduler.schedule(this::forceRotate, props.getRotateAfterHours(), TimeUnit.HOURS);

                        // Отправляем все отложенные подписки РАЗОМ (если есть)
                        sendAllCurrentSubscriptions();
                    }
                });
    }
    private void sendAllCurrentSubscriptions() {
        WebSocket s = this.ws;
        if (s == null) return;
        if (subs.isEmpty()) return;
        try {
            sendText(buildCmd("SUBSCRIPTION", subs.toArray(String[]::new)));
            log.info("📬 SUB (flush queued) {}", String.join(", ", subs));
        } catch (Exception e) {
            log.warn("Failed to flush queued subscriptions: {}", e.toString());
        }
    }

    public synchronized void close() {
        try { if (ws != null) ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye"); } catch (Exception ignore) {}
        scheduler.shutdownNow();
    }

    // ---- subscriptions ----
    public void subscribe(String... channels) {
        if (channels == null || channels.length == 0) return;
        for (String ch : channels) subs.add(ch);

        WebSocket s = this.ws;
        if (s == null) {
            log.info("🕗 queued SUB {}", String.join(", ", channels));
            return; // отправим после коннекта
        }

        sendText(buildCmd("SUBSCRIPTION", channels));
        log.info("📬 SUB {}", String.join(", ", channels));

        if (subs.size() > props.getMaxSubscriptionsPerConn()) {
            log.warn("⚠️ >{} subscriptions on single WS", props.getMaxSubscriptionsPerConn());
        }
    }


    public void unsubscribe(String... channels) {
        if (channels == null || channels.length == 0) return;
        for (String ch : channels) subs.remove(ch);

        WebSocket s = this.ws;
        if (s == null) {
            log.info("🕗 queued UNSUB {}", String.join(", ", channels));
            return; // пошлём при следующем коннекте, если будет смысл
        }

        sendText(buildCmd("UNSUBSCRIPTION", channels));
        log.info("🗑  UNSUB {}", String.join(", ", channels));
    }


    private void safePing() { try { sendText("{\"method\":\"PING\"}"); } catch (Exception ignore) {} }

    private static String buildCmd(String method, String[] params) {
        StringBuilder sb = new StringBuilder("{\"method\":\"").append(method).append("\",\"params\":[");
        for (int i = 0; i < params.length; i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(params[i].replace("\"","\\\"")).append('"');
        }
        return sb.append("]}").toString();
    }

    private synchronized void sendText(String json) {
        if (ws == null) throw new IllegalStateException("WS not connected");
        ws.sendText(json, true);
    }

    private void forceRotate() {
        try {
            if (ws != null) {
                log.info("♻️ Rotate before 24h limit");
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "rotate");
            }
        } catch (Exception ignore) {}
    }

    private void scheduleReconnect(int attempt) {
        if (!reconnecting.compareAndSet(false, true)) return;
        long base = props.getReconnectBackoffBaseSec();
        long max = props.getReconnectBackoffMaxSec();
        long delay = Math.min(max, (long) (base * Math.pow(2, attempt - 1)));
        log.info("⏳ Reconnect in {} sec (attempt #{})", delay, attempt);
        scheduler.schedule(() -> {
            try { connect(); } finally { reconnecting.set(false); }
        }, delay, TimeUnit.SECONDS);
    }

    // ---- WebSocket.Listener ----
    @Override public void onOpen(WebSocket webSocket) { webSocket.request(1); }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        String s = data.toString();
        if (s.contains("PONG")) {
            fire(Listener::onPong);
        } else {
            fire(l -> l.onAck(s));
        }
        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer message, boolean last) {
        byte[] bytes = new byte[message.remaining()];
//        log.debug("BIN frame size={} bytes", bytes.length);
        message.get(bytes);

        try {
            PushDataV3ApiWrapper w = PushDataV3ApiWrapper.parseFrom(bytes);
//            log.debug("WRAPPER toString: {}", w); // покажет, какие поля реально выставлены
            String symbol = w.getSymbol();
            long sendTime = w.getSendTime();
            // --- PRIVATE (user data) ---
            if (w.hasPrivateAccount()) {
                fire(l -> l.onPrivateAccount(w.getPrivateAccount(), sendTime));
            } else if (w.hasPrivateDeals()) {
                fire(l -> l.onPrivateDeals(symbol, w.getPrivateDeals(), sendTime));
            } else if (w.hasPrivateOrders()) {
                fire(l -> l.onPrivateOrders(symbol, w.getPrivateOrders(), sendTime));
            }
            // MARKET (market)
            else if (w.hasPublicAggreDeals()) {
                fire(l -> l.onDeals(symbol, w.getPublicAggreDeals(), sendTime));
            } else if (w.hasPublicIncreaseDepths()) {
                fire(l -> l.onDepthInc(symbol, w.getPublicIncreaseDepths(), sendTime));
            } else if (w.hasPublicLimitDepths()) {
                fire(l -> l.onLimitDepth(symbol, w.getPublicLimitDepths(), sendTime));
            } else if (w.hasPublicAggreBookTicker()) {
                fire(l -> l.onBookTicker(symbol, w.getPublicAggreBookTicker(), sendTime));
            } else if (w.hasPublicMiniTicker()) {
                fire(l -> l.onMiniTicker(symbol, w.getPublicMiniTicker(), sendTime));
            } else if (w.hasPublicMiniTickers()) {
                fire(l -> l.onMiniTickers(w.getPublicMiniTickers(), sendTime));
            } else if (w.hasPublicSpotKline()) {
                fire(l -> l.onKline(symbol, w.getPublicSpotKline(), w.getCreateTime()));
            }
        } catch (InvalidProtocolBufferException e) {
            log.warn("✖️ PB parse: {}", e.toString());
            fire(l -> l.onError(e));
        }

        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    @Override public void onError(WebSocket webSocket, Throwable error) {
        log.warn("💥 WS error: {}", error.toString());
        fire(l -> l.onError(error));
        scheduleReconnect(1);
    }

    @Override public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.info("🔒 WS closed: {} {}", statusCode, reason);
        fire(l -> l.onClosed(statusCode, reason));
        scheduleReconnect(1);
        return CompletableFuture.completedFuture(null);
    }

    private void fire(Consumer<Listener> c) {
        for (Listener l : listeners) {
            try { c.accept(l); } catch (Throwable ignore) {}
        }
    }
}


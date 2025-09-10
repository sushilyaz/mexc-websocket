package com.suhoi.mexcwebsocket.mexc.ws.user;

import com.suhoi.mexcwebsocket.domain.OrderStateTracker;
import com.suhoi.mexcwebsocket.infra.OrderEventBus;
import com.suhoi.mexcwebsocket.mexc.ws.core.MexcWsClient;
import com.suhoi.mexcwebsocket.config.MexcWsProps;
import com.suhoi.mexcwebsocket.domain.model.Creds;
import com.mxc.push.common.protobuf.PrivateAccountV3Api;
import com.mxc.push.common.protobuf.PrivateDealsV3Api;
import com.mxc.push.common.protobuf.PrivateOrdersV3Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.suhoi.mexcwebsocket.mexc.rest.MexcRestClient.bd;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserStreamRegistry {

    public enum Slot { A, B }

    private final MexcWsProps baseWsProps;          // для пингов/ротации
    private final ListenKeyClient listenKeyClient;  // твой рабочий класс
    private final OrderEventBus orderEventBus;
    private final OrderStateTracker orderStateTracker;
    /** key = chatId#slot  -> сессия */
    private final Map<String, UserStreamSession> sessions = new ConcurrentHashMap<>();

    /** Создать или перезапустить сессию, если креды изменились. */
    public synchronized void startOrUpdate(long chatId, Slot slot, Creds creds) {
        Objects.requireNonNull(creds, "creds");

        String key = key(chatId, slot);
        UserStreamSession existing = sessions.get(key);
        if (existing != null && existing.sameCreds(creds)) {
            log.info("[USER:{}:{}] already running", chatId, slot);
            return;
        }
        // стопим старую (если была)
        if (existing != null) {
            log.info("[USER:{}:{}] replacing session (creds changed)", chatId, slot);
            existing.stop();
        }

        // создаём новую сессию
        UserStreamSession session = new UserStreamSession(
                baseWsProps, listenKeyClient, creds,
                new ForwardingListener(chatId, slot, orderEventBus, orderStateTracker) // <- передаём
        );
        session.startAndSubscribe(); // откроет WS с listenKey и подпишется на приватные каналы
        sessions.put(key, session);
        log.info("[USER:{}:{}] started", chatId, slot);
    }

    /** Остановить сессию. */
    public synchronized void stop(long chatId, Slot slot) {
        String key = key(chatId, slot);
        UserStreamSession s = sessions.remove(key);
        if (s != null) {
            s.stop();
            log.info("[USER:{}:{}] stopped", chatId, slot);
        }
    }

    /** Остановить обе сессии для чата. */
    public synchronized void stopAll(long chatId) {
        stop(chatId, Slot.A);
        stop(chatId, Slot.B);
    }

    private static String key(long chatId, Slot slot) { return chatId + "#" + slot; }

    /** Слушатель, который просто красиво логирует, но тут можно дергать Telegram-бота. */
    private static final class ForwardingListener implements MexcWsClient.Listener {
        private final long chatId;
        private final Slot slot;
        private final OrderEventBus bus;
        private final OrderStateTracker tracker;

        public ForwardingListener(long chatId, Slot slot, OrderEventBus bus, OrderStateTracker tracker) {
            this.chatId = chatId;
            this.slot = slot;
            this.bus = bus;
            this.tracker = tracker;
        }

        @Override public void onOpen() { log.info("✅ USER[{}:{}] WS OPEN", chatId, slot); }
        @Override public void onAck(String json) { log.info("✅ USER[{}:{}] ACK {}", chatId, slot, json); }
        @Override public void onPong() { log.debug("🏓 USER[{}:{}] PONG", chatId, slot); }

        @Override
        public void onPrivateAccount(PrivateAccountV3Api acc, long ts) {
            log.info("👤 [{}:{}] {} bal={} (Δ={}) frozen={} (Δ={}) type={} t={}",
                    chatId, slot, acc.getVcoinName(),
                    acc.getBalanceAmount(), acc.getBalanceAmountChange(),
                    acc.getFrozenAmount(), acc.getFrozenAmountChange(),
                    acc.getType(), acc.getTime());
        }

        @Override
        public void onPrivateDeals(String symbol, PrivateDealsV3Api d, long ts) {
            log.info("💼 [{}:{}] {} px={} qty={} amt={} side={} fee={} {} tradeId={}",
                    chatId, slot, symbol,
                    d.getPrice(), d.getQuantity(), d.getAmount(), d.getTradeType(),
                    d.getFeeAmount(), d.getFeeCurrency(), d.getTradeId());
            tracker.onEvent(new com.suhoi.mexcwebsocket.domain.events.OrderEvent.OrderTrade(
                    symbol,
                    d.getClientOrderId(),         // важно: clientId есть в deals
                    null,                    // orderId может не приходить — ок
                    java.time.Instant.ofEpochMilli(ts),
                    bd(d.getPrice()), bd(d.getQuantity()), bd(d.getAmount()),
                    d.getTradeType(), bd(d.getFeeAmount()), d.getFeeCurrency(), d.getTradeId()
            ));
        }

        @Override
        public void onPrivateOrders(String symbol, PrivateOrdersV3Api o, long ts) {
            log.info("📜 [{}:{}] {} id={} type={} side={} px={} qty={} filled={} avgPx={} status={} t={}",
                    chatId, slot, symbol, o.getClientId(), o.getOrderType(), o.getTradeType(),
                    o.getPrice(), o.getQuantity(), o.getCumulativeQuantity(), o.getAvgPrice(),
                    o.getStatus(), o.getCreateTime());
            // Маппинг статуса: 1=NEW, 2=FILLED, 3=PARTIALLY_FILLED, 4=CANCELED, 5=PARTIALLY_CANCELED
            int st = o.getStatus();
            var tsInstant = java.time.Instant.ofEpochMilli(ts);

            switch (st) {
                case 2 -> tracker.onEvent(new com.suhoi.mexcwebsocket.domain.events.OrderEvent.OrderFilled(
                        symbol, o.getClientId(), null, tsInstant,
                        bd(o.getCumulativeQuantity()), bd(o.getCumulativeAmount()), bd(o.getAvgPrice())));
                case 3 -> tracker.onEvent(new com.suhoi.mexcwebsocket.domain.events.OrderEvent.OrderPartiallyFilled(
                        symbol, o.getClientId(), null, tsInstant,
                        bd(o.getCumulativeQuantity()), bd(o.getCumulativeAmount()), bd(o.getAvgPrice())));
                case 4, 5 -> tracker.onEvent(new com.suhoi.mexcwebsocket.domain.events.OrderEvent.OrderCanceled(
                        symbol, o.getClientId(), null, tsInstant,
                        bd(o.getCumulativeQuantity()), bd(o.getCumulativeAmount()), bd(o.getAvgPrice())));
                default -> tracker.onEvent(new com.suhoi.mexcwebsocket.domain.events.OrderEvent.OrderAccepted(
                        symbol, o.getClientId(), null, tsInstant,
                        bd(o.getPrice()), bd(o.getQuantity()), o.getTradeType(), o.getOrderType()));
            }
        }
        // мини-хелпер (можешь убрать и юзать свой bd)
        private static java.math.BigDecimal bd(String s) {
            try { return new java.math.BigDecimal(s); } catch (Exception e) { return java.math.BigDecimal.ZERO; }
        }
    }
}

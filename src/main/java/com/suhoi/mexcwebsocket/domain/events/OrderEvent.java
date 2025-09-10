package com.suhoi.mexcwebsocket.domain.events;

import java.math.BigDecimal;
import java.time.Instant;

/** Базовый тип всех событий ордера (event-driven модель). */
public sealed interface OrderEvent permits
        OrderEvent.OrderAccepted,
        OrderEvent.OrderPartiallyFilled,
        OrderEvent.OrderFilled,
        OrderEvent.OrderCanceled,
        OrderEvent.OrderRejected,
        OrderEvent.OrderTrade
{
    String symbol();
    String clientId();           // твой newClientOrderId (ключ корреляции)
    String orderId();            // может быть null (если нет в WS-сообщении)
    Instant ts();

    /** Биржа приняла команду (можно эмитить при первом WS или сразу после POST, на твой вкус). */
    record OrderAccepted(String symbol, String clientId, String orderId, Instant ts,
                         BigDecimal price, BigDecimal qty, int side, int type) implements OrderEvent {}

    /** Частичное исполнение. */
    record OrderPartiallyFilled(String symbol, String clientId, String orderId, Instant ts,
                                BigDecimal cumulativeQty, BigDecimal cumulativeQuote, BigDecimal avgPrice) implements OrderEvent {}

    /** Полное исполнение. */
    record OrderFilled(String symbol, String clientId, String orderId, Instant ts,
                       BigDecimal cumulativeQty, BigDecimal cumulativeQuote, BigDecimal avgPrice) implements OrderEvent {}

    /** Отмена (включая частично отменённый по твоему желанию отдельным типом). */
    record OrderCanceled(String symbol, String clientId, String orderId, Instant ts,
                         BigDecimal cumulativeQty, BigDecimal cumulativeQuote, BigDecimal avgPrice) implements OrderEvent {}

    /** Отказ биржи. */
    record OrderRejected(String symbol, String clientId, String orderId, Instant ts,
                         String reason) implements OrderEvent {}

    /** Сделка (fill-часть), удобно для подробной телеметрии/комиссий. */
    record OrderTrade(String symbol, String clientId, String orderId, Instant ts,
                      BigDecimal price, BigDecimal qty, BigDecimal amount,
                      int side, BigDecimal fee, String feeCcy, String tradeId) implements OrderEvent {}
}

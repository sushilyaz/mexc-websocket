// application/DrainService.java
package com.suhoi.mexcwebsocket.application;

import com.suhoi.mexcwebsocket.db.MemoryDb;
import com.suhoi.mexcwebsocket.domain.OrderStateTracker;
import com.suhoi.mexcwebsocket.domain.events.OrderEvent;
import com.suhoi.mexcwebsocket.domain.model.Creds;
import com.suhoi.mexcwebsocket.domain.model.DrainSession;
import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import com.suhoi.mexcwebsocket.infra.OrderEventBus;
import com.suhoi.mexcwebsocket.mexc.rest.MexcRestFacade;
import com.suhoi.mexcwebsocket.mexc.ws.market.MexcWsFacade;
import com.suhoi.mexcwebsocket.mexc.ws.market.OrderBookService;
import com.suhoi.mexcwebsocket.mexc.ws.user.UserStreamRegistry;
import com.suhoi.mexcwebsocket.util.MarketMath;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.suhoi.mexcwebsocket.util.FormatHelpers.fmt;

@Service
@Slf4j
@RequiredArgsConstructor
public class DrainService {

    private final UserStreamRegistry userStreams;
    private final OrderBookService orderBooks;
    private final MexcRestFacade mexcRestFacade;
    private final OrderEventBus orderEventBus;     // шина событий
    private final OrderStateTracker tracker;       // пригодится дальше (пока не используем)
    private final ScheduledExecutorService drainScheduler; // общий планировщик таймаутов
    private static final long SELL_STATUS_TIMEOUT_MS = 3000; // можно 0, если вообще без страховок

    private final MexcWsFacade mexcWsFacade;

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId) {
        Creds credsA = MemoryDb.getAccountA(chatId);
        Creds credsB = MemoryDb.getAccountB(chatId);

        log.info("🚀 START_DRAIN chatId={} symbol='{}' amount={} USDT", chatId, symbol, fmt(usdtAmount));

        // 1) гарантируем приватные WS по A/B
        if (credsA != null) userStreams.startOrUpdate(chatId, UserStreamRegistry.Slot.A, credsA);
        else log.warn("⚠️ No creds for AccountA (chatId={})", chatId);
        if (credsB != null) userStreams.startOrUpdate(chatId, UserStreamRegistry.Slot.B, credsB);
        else log.warn("⚠️ No creds for AccountB (chatId={})", chatId);

        // 2) включаем L2 для символа
        if (symbol != null && !symbol.isBlank()) {
            orderBooks.startTracking(symbol);
            log.info("🧱 L2 initialized for {}", symbol.toUpperCase());
        } else {
            log.warn("⚠️ Symbol is empty — L2 not started");
        }

        // 3) создаём сессию
        DrainSession s = new DrainSession();
        s.setSymbol(symbol);
        s.setState(DrainSession.State.IDLE);
        MemoryDb.setSession(chatId, s);

        // 4) генерируем clientId для агрессивной покупки A (LIMIT IOC над спредом)
        String buyAClientId = UUID.randomUUID().toString().replace("-", "");
        s.setBuyOrderId(buyAClientId);

        // 5) подписка на события по этому clientId
        var subRef = new AtomicReference<OrderEventBus.Subscription>();
        OrderEventBus.Subscription sub = orderEventBus.subscribe(ev -> {
            // фильтр byClientId уже применён в subscribe(...)
            if (ev instanceof OrderEvent.OrderPartiallyFilled p) {
                log.info("📥 {} BUY[A] PARTIAL cid={} cumQty={} avg={} quote={}",
                        symbol, p.clientId(),
                        p.cumulativeQty().stripTrailingZeros(),
                        p.avgPrice().stripTrailingZeros(),
                        p.cumulativeQuote().stripTrailingZeros());

            } else if (ev instanceof OrderEvent.OrderFilled f) {
                s.setQtyA(f.cumulativeQty());
                s.setState(DrainSession.State.A_MKT_BUY_DONE);
                log.info("✅ {} BUY[A] FILLED cid={} qtyA={} avg={} quote={}",
                        symbol, f.clientId(),
                        s.getQtyA().stripTrailingZeros(),
                        f.avgPrice().stripTrailingZeros(),
                        f.cumulativeQuote().stripTrailingZeros());

                var old = subRef.getAndSet(null);
                if (old != null) old.close();

                MemoryDb.setSession(chatId, s);
                // TODO: запустить цикл:
                executeCycle(chatId, s);

            } else if (ev instanceof OrderEvent.OrderCanceled c) {
                // типичный итог для IOC: частично исполнил → остаток отменён
                if (c.cumulativeQty() != null && c.cumulativeQty().signum() > 0) {
                    s.setQtyA(c.cumulativeQty());
                    s.setState(DrainSession.State.A_MKT_BUY_DONE);
                    log.info("✅ {} BUY[A] IOC PARTIAL→CANCELED cid={} qtyA={} avg={}",
                            symbol, c.clientId(),
                            s.getQtyA().stripTrailingZeros(),
                            c.avgPrice().stripTrailingZeros());

                    var old = subRef.getAndSet(null);
                    if (old != null) old.close();

                    MemoryDb.setSession(chatId, s);
                    // TODO: следующий шаг: стартуем цикл
                } else {
                    s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "IOC buy filled=0");
                    var old = subRef.getAndSet(null);
                    if (old != null) old.close();
                }

            } else if (ev instanceof OrderEvent.OrderRejected r) {
                s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "A BUY rejected: " + r.reason());
                var old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, OrderEventBus.byClientId(buyAClientId));
        subRef.set(sub);

        // 6) таймаут ожидания статуса по заявке A (например, 3 сек)
        drainScheduler.schedule(() -> {
            if (subRef.get() != null && s.getState() != DrainSession.State.A_MKT_BUY_DONE) {
                s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY");
                var old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, 3, TimeUnit.SECONDS);

        // 7) команда на покупку (REST без внутренних подписок)
        String orderId = null;
        try {
            orderId = mexcRestFacade.limitBuyAboveSpreadA(symbol, usdtAmount, chatId, buyAClientId);
        } catch (Exception ex) {
            log.error("A BUY failed: {}", ex.getMessage(), ex);
        }
        if (orderId == null) {
            // REST-команда не принята — снимаем ожидание и автопауза
            s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "A BUY REST failed/null orderId");
            var old = subRef.getAndSet(null);
            if (old != null) old.close();
            return;
        }

        log.info("A BUY placed: symbol={} clientId={} orderId={}", symbol, buyAClientId, orderId);
    }

    // DrainService.java

    private BigDecimal executeCycle(Long chatId, DrainSession s) {
        final String symbol = s.getSymbol();

        SymbolFilters f = mexcRestFacade.getSymbolFilters(symbol);
        BigDecimal pSell = mexcWsFacade.getNearLowerSpreadPrice(symbol);
        BigDecimal minQtyForSell = MarketMath.minQtyForNotional(pSell, f.getStepSize(), f.getMinNotional());

        if (s.getQtyA() == null || s.getQtyA().compareTo(minQtyForSell) < 0) {
            return autoPauseAndZero(
                    s,
                    DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                    "qtyA < minNotional для SELL @ " + fmt(pSell) + " (qtyA=" + fmt(s.getQtyA()) + ", min=" + fmt(minQtyForSell) + ")",
                    "PRE-A-SELL-MIN"
            );
        }

        log.info("[SELL_PLANNED] {} nearSell={} planQtyA={}", symbol, fmt(pSell), fmt(s.getQtyA()));
        final Map<BigDecimal, BigDecimal> myAsks = new ConcurrentHashMap<>();

        String sellClientId = UUID.randomUUID().toString().replace("-", "");
        s.setSellOrderId(sellClientId);
        s.setPSell(pSell);
        s.setState(DrainSession.State.A_SELL_PLACED);

        var subRef = new AtomicReference<OrderEventBus.Subscription>();
        OrderEventBus.Subscription sub = orderEventBus.subscribe(ev -> {

            if (ev instanceof OrderEvent.OrderAccepted a) {
                BigDecimal qtyPlanned = MarketMath.normalizeQty(s.getQtyA(), f);
                String buyBClientId = UUID.randomUUID().toString().replace("-", "");

                myAsks.clear();
                myAsks.put(s.getPSell(), qtyPlanned);

                var subBRef = new AtomicReference<OrderEventBus.Subscription>();
                OrderEventBus.Subscription subB = orderEventBus.subscribe(evB -> {
                    if (evB instanceof OrderEvent.OrderPartiallyFilled pb) {
                        log.info("📥 {} BUY[B] PARTIAL cid={} cumQty={} avg={} quote={}",
                                symbol, pb.clientId(),
                                pb.cumulativeQty().stripTrailingZeros(),
                                pb.avgPrice().stripTrailingZeros(),
                                pb.cumulativeQuote().stripTrailingZeros());

                    } else if (evB instanceof OrderEvent.OrderFilled fb) {
                        log.info("✅ {} BUY[B] FILLED cid={} cumQty={} avg={} quote={}",
                                symbol, fb.clientId(),
                                fb.cumulativeQty().stripTrailingZeros(),
                                fb.avgPrice().stripTrailingZeros(),
                                fb.cumulativeQuote().stripTrailingZeros());

                        s.setLastSpentB(fb.cumulativeQuote());
                        OrderEventBus.Subscription oldB = subBRef.getAndSet(null);
                        if (oldB != null) oldB.close();

                    } else if (evB instanceof OrderEvent.OrderCanceled cb) {
                        BigDecimal filled = cb.cumulativeQty() == null ? BigDecimal.ZERO : cb.cumulativeQty();
                        if (filled.compareTo(qtyPlanned) < 0) {
                            s.autoPause(DrainSession.AutoPauseReason.FRONT_RUN,
                                    "BUY[B] filled=" + fmt(filled) + " < planned=" + fmt(qtyPlanned));
                        }
                        OrderEventBus.Subscription oldB = subBRef.getAndSet(null);
                        if (oldB != null) oldB.close();

                    } else if (evB instanceof OrderEvent.OrderRejected rb) {
                        s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "B BUY rejected: " + rb.reason());
                        OrderEventBus.Subscription oldB = subBRef.getAndSet(null);
                        if (oldB != null) oldB.close();
                    }
                }, OrderEventBus.byClientId(buyBClientId));
                subBRef.set(subB);

                drainScheduler.schedule(() -> {
                    if (subBRef.get() != null && s.getState() != DrainSession.State.AUTO_PAUSE) {
                        s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for B BUY");
                        OrderEventBus.Subscription oldB = subBRef.getAndSet(null);
                        if (oldB != null) oldB.close();
                    }
                }, 3, TimeUnit.SECONDS);

                try {
                    String oidB = mexcRestFacade.limitBuyAboveSpreadB(symbol, s.getPSell(), qtyPlanned, chatId, buyBClientId);
                    if (oidB == null) {
                        s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "B BUY REST failed/null orderId");
                        OrderEventBus.Subscription oldB = subBRef.getAndSet(null);
                        if (oldB != null) oldB.close();
                    } else {
                        s.setState(DrainSession.State.B_MKT_BUY_SENT);
                        log.info("➡️ {} BUY[B] placed: cidB={} orderIdB={} qtyPlanned={}",
                                symbol, buyBClientId, oidB, fmt(qtyPlanned));
                    }
                } catch (Exception ex) {
                    log.error("BUY[B] send failed: {}", ex.getMessage(), ex);
                    s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "B MARKET-LIKE BUY send failed");
                    OrderEventBus.Subscription oldB = subBRef.getAndSet(null);
                    if (oldB != null) oldB.close();
                }

            } else if (ev instanceof OrderEvent.OrderPartiallyFilled p) {
                log.info("📥 {} SELL[A] PARTIAL cid={} cumQty={} avg={} quote={}",
                        symbol, p.clientId(),
                        p.cumulativeQty().stripTrailingZeros(),
                        p.avgPrice().stripTrailingZeros(),
                        p.cumulativeQuote().stripTrailingZeros());

            } else if (ev instanceof OrderEvent.OrderFilled fEv) {
                log.info("✅ {} SELL[A] FILLED cid={} cumQty={} avg={} quote={}",
                        symbol, fEv.clientId(),
                        fEv.cumulativeQty().stripTrailingZeros(),
                        fEv.avgPrice().stripTrailingZeros(),
                        fEv.cumulativeQuote().stripTrailingZeros());

                s.setState(DrainSession.State.A_SELL_FILLED);
                s.setLastCummA(fEv.cumulativeQuote()); // сколько USDT пришло на A

                OrderEventBus.Subscription old = subRef.getAndSet(null);
                if (old != null) old.close();

                // === [UPPER-LEG] ===
                try {
                    BigDecimal pBuyUpper = mexcWsFacade.getNearUpperSpreadPriceExcludingMine(
                            symbol,
                            java.util.Collections.emptyMap(),  // myBids
                            myAsks                              // myAsks (исключаем наш SELL[A])
                    );
                    s.setPBuy(pBuyUpper);
                    log.info("[BUY_UPPER_PLANNED/X] {} nearBuy={} (исключая наш SELL[A] @ {})",
                            symbol, fmt(pBuyUpper), fmt(s.getPSell()));

                    // Объём: симметрично — сколько держит B (это то, что мы только что продали)
                    BigDecimal qtyUpper = MarketMath.normalizeQty(s.getQtyA(), f);

                    // Проверим minNotional/minQty на pBuyUpper (как и в первой ноге)
                    BigDecimal effMinNotional = MarketMath.resolveMinNotional(symbol, f.getMinNotional());
                    BigDecimal minQtyNeedUpper = MarketMath.minQtyForNotional(pBuyUpper, f.getStepSize(), effMinNotional);
                    if (qtyUpper.compareTo(minQtyNeedUpper) < 0 || qtyUpper.compareTo(f.getMinQty()) < 0) {
                        s.autoPause(DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                                "qtyUpper < minNotional для BUY[A] @ " + fmt(pBuyUpper) +
                                        " (qtyUpper=" + fmt(qtyUpper) + ", min=" + fmt(minQtyNeedUpper) + ")");
                        return;
                    }

                    // Подписка на BUY[A] @ верхней кромке
                    String buyAUpperClientId = UUID.randomUUID().toString().replace("-", "");
                    var subAUpperRef = new AtomicReference<OrderEventBus.Subscription>();
                    OrderEventBus.Subscription subAUpper = orderEventBus.subscribe(evA -> {
                        if (evA instanceof OrderEvent.OrderAccepted acc) {
                            // как только BUY[A] принят — сразу «обходной» SELL[B] (IOC) в него
                            String sellBBelowClientId = UUID.randomUUID().toString().replace("-", "");

                            var subSellBRef = new AtomicReference<OrderEventBus.Subscription>();
                            OrderEventBus.Subscription subSellB = orderEventBus.subscribe(evB2 -> {
                                if (evB2 instanceof OrderEvent.OrderPartiallyFilled pp) {
                                    log.info("📥 {} SELL[B] PARTIAL cid={} cumQty={} avg={} quote={}",
                                            symbol, pp.clientId(),
                                            pp.cumulativeQty().stripTrailingZeros(),
                                            pp.avgPrice().stripTrailingZeros(),
                                            pp.cumulativeQuote().stripTrailingZeros());
                                } else if (evB2 instanceof OrderEvent.OrderFilled ff) {
                                    log.info("✅ {} SELL[B] FILLED cid={} cumQty={} avg={} quote={} (учтена тейкерская комиссия в USDT)",
                                            symbol, ff.clientId(),
                                            ff.cumulativeQty().stripTrailingZeros(),
                                            ff.avgPrice().stripTrailingZeros(),
                                            ff.cumulativeQuote().stripTrailingZeros());
                                    OrderEventBus.Subscription oldB2 = subSellBRef.getAndSet(null);
                                    if (oldB2 != null) oldB2.close();
                                } else if (evB2 instanceof OrderEvent.OrderCanceled cc) {
                                    log.warn("❗ {} SELL[B] IOC canceled (cum={})", symbol, fmt(cc.cumulativeQty()));
                                    OrderEventBus.Subscription oldB2 = subSellBRef.getAndSet(null);
                                    if (oldB2 != null) oldB2.close();
                                } else if (evB2 instanceof OrderEvent.OrderRejected rr) {
                                    s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "B SELL rejected: " + rr.reason());
                                    OrderEventBus.Subscription oldB2 = subSellBRef.getAndSet(null);
                                    if (oldB2 != null) oldB2.close();
                                }
                            }, OrderEventBus.byClientId(sellBBelowClientId));
                            subSellBRef.set(subSellB);

                            // таймаут на SELL[B]
                            drainScheduler.schedule(() -> {
                                if (subSellBRef.get() != null && s.getState() != DrainSession.State.AUTO_PAUSE) {
                                    s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for B SELL");
                                    OrderEventBus.Subscription oldB2 = subSellBRef.getAndSet(null);
                                    if (oldB2 != null) oldB2.close();
                                }
                            }, 3, TimeUnit.SECONDS);

                            try {
                                String oidSellB = mexcRestFacade.limitSellBelowSpreadB(
                                        symbol, s.getPBuy(), qtyUpper, chatId, sellBBelowClientId);
                                if (oidSellB == null) {
                                    s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "B SELL REST failed/null orderId");
                                    OrderEventBus.Subscription oldB2 = subSellBRef.getAndSet(null);
                                    if (oldB2 != null) oldB2.close();
                                } else {
                                    log.info("➡️ {} SELL[B] placed: cidB={} orderIdB={} qty={}",
                                            symbol, sellBBelowClientId, oidSellB, fmt(qtyUpper));
                                }
                            } catch (Exception ex) {
                                log.error("SELL[B] send failed: {}", ex.getMessage(), ex);
                                s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "B MARKET-LIKE SELL send failed");
                                OrderEventBus.Subscription oldB2 = subSellBRef.getAndSet(null);
                                if (oldB2 != null) oldB2.close();
                            }

                        } else if (evA instanceof OrderEvent.OrderFilled fA) {
                            log.info("✅ {} BUY[A] FILLED cid={} cumQty={} avg={} quote={}",
                                    symbol, fA.clientId(),
                                    fA.cumulativeQty().stripTrailingZeros(),
                                    fA.avgPrice().stripTrailingZeros(),
                                    fA.cumulativeQuote().stripTrailingZeros());
                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();

                            // тут можно стартовать следующий цикл (если хочешь зациклить)

                        } else if (evA instanceof OrderEvent.OrderCanceled cA) {
                            log.warn("❗ {} BUY[A] canceled (cum={})", symbol, fmt(cA.cumulativeQty()));
                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();

                        } else if (evA instanceof OrderEvent.OrderRejected rA) {
                            s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "A BUY rejected: " + rA.reason());
                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();
                        }
                    }, OrderEventBus.byClientId(buyAUpperClientId));
                    subAUpperRef.set(subAUpper);

                    // таймаут на BUY[A] верхней кромки
                    drainScheduler.schedule(() -> {
                        if (subAUpperRef.get() != null && s.getState() != DrainSession.State.AUTO_PAUSE) {
                            s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY@upper");
                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();
                        }
                    }, 3, TimeUnit.SECONDS);

                    // Отправляем лимитную BUY[A] по верхней кромке
                    mexcRestFacade.placeLimitBuyAAt(symbol, pBuyUpper, qtyUpper, chatId, buyAUpperClientId);

                } catch (Exception ex) {
                    log.warn("[BUY_UPPER_PLANNED/X] {} failed: {}", symbol, ex.toString());
                }

            } else if (ev instanceof OrderEvent.OrderCanceled c) {
                s.autoPause(DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                        "SELL canceled (cum=" + fmt(c.cumulativeQty()) + ")");
                OrderEventBus.Subscription old = subRef.getAndSet(null);
                if (old != null) old.close();

            } else if (ev instanceof OrderEvent.OrderRejected r) {
                s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "A SELL rejected: " + r.reason());
                OrderEventBus.Subscription old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, OrderEventBus.byClientId(sellClientId));
        subRef.set(sub);

        drainScheduler.schedule(() -> {
            if (subRef.get() != null && s.getState() != DrainSession.State.A_SELL_FILLED && s.getState() != DrainSession.State.AUTO_PAUSE) {
                s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY");
                var old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, 3, TimeUnit.SECONDS);

        mexcRestFacade.placeLimitSellA(s.getSymbol(), pSell, s.getQtyA(), chatId, sellClientId);
        return null;
    }


    /**
     * Унифицированная автопауза + подробный лог в консоль.
     */
    private BigDecimal autoPauseAndZero(DrainSession s,
                                        DrainSession.AutoPauseReason reason,
                                        String details,
                                        String whereTag) {
        s.autoPause(reason, details);
        log.warn("⏸ AUTO_PAUSE@{} -> reason={} | details={} | {}", whereTag, reason, details, snapshot(s));
        return BigDecimal.ZERO;
    }
    private String snapshot(DrainSession s) {
        if (s == null) return "{session=null}";
        return new StringBuilder(256)
                .append("{state=").append(s.getState())
                .append(", cycle=").append(s.getCycleIndex())
                .append(", symbol=").append(s.getSymbol())
                .append(", qtyA=").append(fmt(s.getQtyA()))
                .append(", pSell=").append(fmt(s.getPSell()))
                .append(", pBuy=").append(fmt(s.getPBuy()))
                .append(", lastSpentB=").append(fmt(s.getLastSpentB()))
                .append(", lastCummA=").append(fmt(s.getLastCummA()))
                .append(", sellOrderId=").append(s.getSellOrderId())
                .append(", buyOrderId=").append(s.getBuyOrderId())
                .append(", reason=").append(s.getReason())
                .append(", details=").append(s.getReasonDetails())
                .append('}')
                .toString();
    }
}

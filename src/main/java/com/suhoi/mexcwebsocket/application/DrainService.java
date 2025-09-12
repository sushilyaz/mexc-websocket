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
import java.util.Collections;
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
    private final OrderEventBus orderEventBus;
    private final OrderStateTracker tracker;
    private final ScheduledExecutorService drainScheduler;
    private static final long SELL_STATUS_TIMEOUT_MS = 3000;
    private static final long GHOST_TTL_MS = 1500;
    private final BalanceControllerWs balanceControllerWs;

    private final MexcWsFacade mexcWsFacade;

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId) {
        Creds credsA = MemoryDb.getAccountA(chatId);
        Creds credsB = MemoryDb.getAccountB(chatId);

        log.info("🚀 START_DRAIN chatId={} symbol='{}' amount={} USDT", chatId, symbol, fmt(usdtAmount));

        if (credsA != null) userStreams.startOrUpdate(chatId, UserStreamRegistry.Slot.A, credsA);
        else log.warn("⚠️ No creds for AccountA (chatId={})", chatId);
        if (credsB != null) userStreams.startOrUpdate(chatId, UserStreamRegistry.Slot.B, credsB);
        else log.warn("⚠️ No creds for AccountB (chatId={})", chatId);

        if (symbol != null && !symbol.isBlank()) {
            orderBooks.startTracking(symbol);
            log.info("🧱 L2 initialized for {}", symbol.toUpperCase());
        } else {
            log.warn("⚠️ Symbol is empty — L2 not started");
        }

        // сессия и цель перелива
        DrainSession s = new DrainSession();
        s.setSymbol(symbol);
        s.setState(DrainSession.State.IDLE);
        s.setTargetDrainUSDT(usdtAmount);           // цель перелива (A -> B)
        s.setDrainedUSDT(BigDecimal.ZERO);          // прогресс
        s.setCycleIndex(0);
        MemoryDb.setSession(chatId, s);

        // первичный закуп аккаунта A (LIMIT IOC над спредом)
        String buyAClientId = UUID.randomUUID().toString().replace("-", "");
        s.setBuyOrderId(buyAClientId);

        var subRef = new AtomicReference<OrderEventBus.Subscription>();
        OrderEventBus.Subscription sub = orderEventBus.subscribe(ev -> {
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
                verifyLater(s, BalanceControllerWs.Phase.AFTER_A_MKT_BUY);

                var old = subRef.getAndSet(null);
                if (old != null) old.close();

                MemoryDb.setSession(chatId, s);
                // старт первого цикла
                executeCycle(chatId, s);

            } else if (ev instanceof OrderEvent.OrderCanceled c) {
                // типично для IOC: частично → остаток cancel
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
                    executeCycle(chatId, s); // всё равно запускаем цикл с фактическим qtyA
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

        drainScheduler.schedule(() -> {
            if (subRef.get() != null && s.getState() != DrainSession.State.A_MKT_BUY_DONE) {
                s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY");
                var old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, 3, TimeUnit.SECONDS);

        String orderId = null;
        try {
            orderId = mexcRestFacade.limitBuyAboveSpreadA(symbol, usdtAmount, chatId, buyAClientId);
        } catch (Exception ex) {
            log.error("A BUY failed: {}", ex.getMessage(), ex);
        }
        if (orderId == null) {
            s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "A BUY REST failed/null orderId");
            var old = subRef.getAndSet(null);
            if (old != null) old.close();
            return;
        }
        log.info("A BUY placed: symbol={} clientId={} orderId={}", symbol, buyAClientId, orderId);
    }

    /**
     * Один полный цикл:
     * A: SELL (нижняя кромка) → B: BUY IOC в него
     * A: BUY (верхняя кромка, исключая мои заявки) → B: SELL IOC в него
     * После FILLED верхнего BUY[A]: учитываем дельту перелива, решаем продолжать/стоп.
     */
    private BigDecimal executeCycle(Long chatId, DrainSession s) {
        final String symbol = s.getSymbol();
        final SymbolFilters f = mexcRestFacade.getSymbolFilters(symbol);

        // карты своих заявок в рамках текущего цикла
        final Map<BigDecimal, BigDecimal> myAsks = new ConcurrentHashMap<>();
        final Map<BigDecimal, BigDecimal> myBids = new ConcurrentHashMap<>();

        // === нижняя кромка: SELL[A] ===
        // исключаем свои потенциальные BID'ы (если вдруг где-то висят из-за гонок/лагов)
        BigDecimal pSell = mexcWsFacade.getNearLowerSpreadPriceExcludingMine(
                symbol,
                myBids,                        // исключаем свои бид-объёмы
                Collections.emptyMap()         // свои аски тут не важны
        );
        BigDecimal effMinNotional = MarketMath.resolveMinNotional(symbol, f.getMinNotional());
        BigDecimal minQtyForSell = MarketMath.minQtyForNotional(pSell, f.getStepSize(), effMinNotional);

        if (s.getQtyA() == null || s.getQtyA().compareTo(minQtyForSell) < 0) {
            return autoPauseAndZero(
                    s,
                    DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                    "qtyA < minNotional для SELL @ " + fmt(pSell) + " (qtyA=" + fmt(s.getQtyA()) + ", min=" + fmt(minQtyForSell) + ")",
                    "PRE-A-SELL-MIN"
            );
        }

        log.info("[SELL_PLANNED] {} nearSell={} planQtyA={}", symbol, fmt(pSell), fmt(s.getQtyA()));

        String sellClientId = UUID.randomUUID().toString().replace("-", "");
        s.setSellOrderId(sellClientId);
        s.setPSell(pSell);
        s.setState(DrainSession.State.A_SELL_PLACED);
        verifyLater(s, BalanceControllerWs.Phase.AFTER_A_SELL_PLACED);

        var subRef = new AtomicReference<OrderEventBus.Subscription>();
        OrderEventBus.Subscription sub = orderEventBus.subscribe(ev -> {

            if (ev instanceof OrderEvent.OrderAccepted a) {
                // как только наш SELL[A] принят — B делает агрессивный BUY IOC в него
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
                // нижняя нога завершена
                log.info("✅ {} SELL[A] FILLED cid={} cumQty={} avg={} quote={}",
                        symbol, fEv.clientId(),
                        fEv.cumulativeQty().stripTrailingZeros(),
                        fEv.avgPrice().stripTrailingZeros(),
                        fEv.cumulativeQuote().stripTrailingZeros());

                s.setState(DrainSession.State.A_SELL_FILLED);
                s.setLastCummA(fEv.cumulativeQuote()); // USDT пришло на A
                s.setLastFilledLowerQty(fEv.cumulativeQty());
                verifyLater(s, BalanceControllerWs.Phase.AFTER_LOWER_FILLED);

                OrderEventBus.Subscription old = subRef.getAndSet(null);
                if (old != null) old.close();

                // === верхняя кромка: BUY[A] (исключая наши заявки), затем SELL[B] IOC в него ===
                try {
                    BigDecimal pBuyUpper = mexcWsFacade.getNearUpperSpreadPriceExcludingMine(
                            symbol,
                            Collections.emptyMap(),  // myBids (пока нет активных)
                            myAsks                   // исключаем наш SELL[A]
                    );
                    s.setPBuy(pBuyUpper);
                    log.info("[BUY_UPPER_PLANNED/X] {} nearBuy={} (исключая наш SELL[A] @ {})",
                            symbol, fmt(pBuyUpper), fmt(s.getPSell()));

                    BigDecimal qtyUpper = MarketMath.normalizeQty(nvl(s.getLastFilledLowerQty()), f);

                    BigDecimal finalEffMinNotional = effMinNotional;
                    finalEffMinNotional = MarketMath.resolveMinNotional(symbol, f.getMinNotional());
                    BigDecimal minQtyNeedUpper = MarketMath.minQtyForNotional(pBuyUpper, f.getStepSize(), finalEffMinNotional);
                    if (qtyUpper.compareTo(minQtyNeedUpper) < 0 || qtyUpper.compareTo(f.getMinQty()) < 0) {
                        s.autoPause(DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                                "qtyUpper < minNotional для BUY[A] @ " + fmt(pBuyUpper) +
                                        " (qtyUpper=" + fmt(qtyUpper) + ", min=" + fmt(minQtyNeedUpper) + ")");
                        return;
                    }

                    String buyAUpperClientId = UUID.randomUUID().toString().replace("-", "");
                    var subAUpperRef = new AtomicReference<OrderEventBus.Subscription>();
                    OrderEventBus.Subscription subAUpper = orderEventBus.subscribe(evA -> {
                        if (evA instanceof OrderEvent.OrderAccepted acc) {
                            // наш BID[A] теперь реально в книге — фиксируем в myBids
                            myBids.clear();
                            myBids.put(s.getPBuy(), qtyUpper);
                            verifyLater(s, BalanceControllerWs.Phase.AFTER_A_BUY_PLACED);

                            // сразу встречная SELL[B] IOC вниз в нашу BUY[A]
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
                                    log.info("✅ {} SELL[B] FILLED cid={} cumQty={} avg={} quote={} (тейкерская комиссия учтена биржей)",
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
                            // ВЕРХНЯЯ НОГА ЗАВЕРШЕНА → считаем дельту перелива, решаем продолжать
                            log.info("✅ {} BUY[A] FILLED cid={} cumQty={} avg={} quote={}",
                                    symbol, fA.clientId(),
                                    fA.cumulativeQty().stripTrailingZeros(),
                                    fA.avgPrice().stripTrailingZeros(),
                                    fA.cumulativeQuote().stripTrailingZeros());

                            // BID[A] ушёл из книги — чистим
                            myBids.clear();

                            // токены A на следующий цикл
                            s.setQtyA(fA.cumulativeQty());
                            s.setLastSpentAUpper(fA.cumulativeQuote());
                            s.setLastFilledUpperQty(fA.cumulativeQty());
                            verifyLater(s, BalanceControllerWs.Phase.AFTER_UPPER_FILLED);

                            // дельта перелива за цикл: (пришло на A при нижнем SELL) - (ушло с A при верхнем BUY)
                            BigDecimal receivedALower = nvl(s.getLastCummA());
                            BigDecimal spentAUpper = nvl(fA.cumulativeQuote());
                            BigDecimal drainedDelta = spentAUpper.subtract(receivedALower); // >=0

                            if (drainedDelta.signum() > 0) {
                                s.setDrainedUSDT(nvl(s.getDrainedUSDT()).add(drainedDelta));
                            }
                            s.setState(DrainSession.State.A_BUY_FILLED);
                            s.setCycleIndex(s.getCycleIndex() + 1);

                            log.info("🔁 CYCLE#{} done: drainedDelta={} USDT, progress={}/{} USDT",
                                    s.getCycleIndex(),
                                    fmt(drainedDelta), fmt(s.getDrainedUSDT()), fmt(s.getTargetDrainUSDT()));

                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();

                            // решить — продолжаем или стоп
                            continueOrFinish(chatId, s, f, myBids);

                        } else if (evA instanceof OrderEvent.OrderCanceled cA) {
                            // отмена верхней ноги — смысла продолжать нет
                            myBids.clear();
                            log.warn("❗ {} BUY[A] canceled (cum={})", symbol, fmt(cA.cumulativeQty()));
                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();
                            s.autoPause(DrainSession.AutoPauseReason.PARTIAL_MISMATCH, "A BUY@upper canceled");

                        } else if (evA instanceof OrderEvent.OrderRejected rA) {
                            myBids.clear();
                            s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "A BUY rejected: " + rA.reason());
                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();
                        }
                    }, OrderEventBus.byClientId(buyAUpperClientId));
                    subAUpperRef.set(subAUpper);

                    drainScheduler.schedule(() -> {
                        if (subAUpperRef.get() != null && s.getState() != DrainSession.State.AUTO_PAUSE) {
                            s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY@upper");
                            OrderEventBus.Subscription oldA = subAUpperRef.getAndSet(null);
                            if (oldA != null) oldA.close();
                        }
                    }, 3, TimeUnit.SECONDS);

                    // лимитная BUY[A] по верхней кромке
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
                s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A SELL");
                var old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, 3, TimeUnit.SECONDS);

        mexcRestFacade.placeLimitSellA(s.getSymbol(), pSell, s.getQtyA(), chatId, sellClientId);
        return null;
    }

    /**
     * Решение о продолжении: цель достигнута? достаточно ли объёма для следующего цикла?
     * Если всё ок — сразу запускаем следующий цикл.
     */
    private void continueOrFinish(Long chatId, DrainSession s, SymbolFilters f, Map<BigDecimal, BigDecimal> myBids) {
        // цель достигнута?
        if (nvl(s.getDrainedUSDT()).compareTo(nvl(s.getTargetDrainUSDT())) >= 0) {
            s.autoPause(DrainSession.AutoPauseReason.MANUAL,
                    "goal reached: drained " + fmt(s.getDrainedUSDT()) + " ≥ target " + fmt(s.getTargetDrainUSDT()));
            log.info("🏁 DONE: {}", snapshot(s));
            return;
        }

        long t0 = System.currentTimeMillis();
        BigDecimal lastBuyPx = s.getPBuy(); // мы раньше ставили BID[A] по этой цене
        while (System.currentTimeMillis() - t0 < GHOST_TTL_MS) {
            BigDecimal bb = orderBooks.bestBid(s.getSymbol());
            if (bb == null || lastBuyPx == null || bb.compareTo(lastBuyPx) != 0) break;
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // хватает ли для следующего нижнего SELL[A] по minNotional?
        BigDecimal nextPSell = mexcWsFacade.getNearLowerSpreadPriceExcludingMine(
                s.getSymbol(),
                (myBids == null ? Collections.emptyMap() : myBids),
                Collections.emptyMap()
        );
        BigDecimal effMinNotional = MarketMath.resolveMinNotional(s.getSymbol(), f.getMinNotional());
        BigDecimal minQtyNextSell = MarketMath.minQtyForNotional(nextPSell, f.getStepSize(), effMinNotional);

        if (s.getQtyA() == null || s.getQtyA().compareTo(minQtyNextSell) < 0) {
            s.autoPause(DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                    "next qtyA < minNotional для SELL @ " + fmt(nextPSell) +
                            " (qtyA=" + fmt(s.getQtyA()) + ", min=" + fmt(minQtyNextSell) + ")");
            log.info("⛔ STOP (minNotional): {}", snapshot(s));
            return;
        }

        // всё ок — следующий цикл
        log.info("🔄 CONTINUE: next pSell={} with qtyA={}", fmt(nextPSell), fmt(s.getQtyA()));
        executeCycle(chatId, s);
    }

    // старый делегат — на всякий случай
    private void continueOrFinish(Long chatId, DrainSession s, SymbolFilters f) {
        continueOrFinish(chatId, s, f, Collections.emptyMap());
    }

    private static BigDecimal nvl(BigDecimal x) {
        return (x == null) ? BigDecimal.ZERO : x;
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

    private void verifyLater(DrainSession s, BalanceControllerWs.Phase ph) {
        // filtros нужны для epsilon/step — безопасно взять их каждый раз из кеша фасада
        var f = mexcRestFacade.getSymbolFilters(s.getSymbol());
        drainScheduler.schedule(() -> balanceControllerWs.verify(s, ph, f), 120, TimeUnit.MILLISECONDS);
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
                .append(", drainedUSDT=").append(fmt(s.getDrainedUSDT()))
                .append(", targetDrainUSDT=").append(fmt(s.getTargetDrainUSDT()))
                .append(", sellOrderId=").append(s.getSellOrderId())
                .append(", buyOrderId=").append(s.getBuyOrderId())
                .append(", reason=").append(s.getReason())
                .append(", details=").append(s.getReasonDetails())
                .append('}')
                .toString();
    }
}

package com.suhoi.mexcwebsocket.application;

import com.suhoi.mexcwebsocket.adapter.telegram.TelegramService;
import com.suhoi.mexcwebsocket.db.MemoryDb;
import com.suhoi.mexcwebsocket.domain.OrderStateTracker;
import com.suhoi.mexcwebsocket.domain.events.OrderEvent;
import com.suhoi.mexcwebsocket.domain.model.BalanceSnapshot;
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
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.suhoi.mexcwebsocket.util.Constants.startBalances;
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
    private static final long GHOST_TTL_MS = 1500;
    private final BalanceControllerWs balanceControllerWs;
    private final MexcWsFacade mexcWsFacade;
    private final TelegramService telegram;

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId) {
        Creds credsA = MemoryDb.getAccountA(chatId);
        Creds credsB = MemoryDb.getAccountB(chatId);
        MemoryDb.getFlag(chatId).set(true);

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

        DrainSession s = new DrainSession();
        s.setSymbol(symbol);
        s.setState(DrainSession.State.IDLE);
        s.setTargetDrainUSDT(usdtAmount);
        s.setDrainedUSDT(BigDecimal.ZERO);
        s.setCycleIndex(0);
        s.setRunId(s.getRunId() + 1);  // новая эпоха
        MemoryDb.setSession(chatId, s);

        mexcRestFacade.captureStartBalanceAccount(symbol, chatId);

        String buyAClientId = UUID.randomUUID().toString().replace("-", "");
        s.setBuyOrderId(buyAClientId);
        final long runIdLocal = s.getRunId();

        var subRef = new AtomicReference<OrderEventBus.Subscription>();
        OrderEventBus.Subscription sub = orderEventBus.subscribe(ev -> {
            if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                closeSub(subRef);
                return;
            }

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

                verifyLater(chatId, s, BalanceControllerWs.Phase.AFTER_A_MKT_BUY);
                closeSub(subRef);

                MemoryDb.setSession(chatId, s);
                executeCycle(chatId, s);

            } else if (ev instanceof OrderEvent.OrderCanceled c) {
                if (c.cumulativeQty() != null && c.cumulativeQty().signum() > 0) {
                    s.setQtyA(c.cumulativeQty());
                    s.setState(DrainSession.State.A_MKT_BUY_DONE);
                    log.info("✅ {} BUY[A] IOC PARTIAL→CANCELED cid={} qtyA={} avg={}",
                            symbol, c.clientId(),
                            s.getQtyA().stripTrailingZeros(),
                            c.avgPrice().stripTrailingZeros());

                    closeSub(subRef);
                    MemoryDb.setSession(chatId, s);
                    executeCycle(chatId, s);

                } else {
                    autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.TIMEOUT, "IOC buy filled=0", "A-BUY-IOC");
                    closeSub(subRef);
                }

            } else if (ev instanceof OrderEvent.OrderRejected r) {
                autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "A BUY rejected: " + r.reason(), "A-BUY-REJ");
                closeSub(subRef);
            }
        }, OrderEventBus.byClientId(buyAClientId));
        subRef.set(sub);

        drainScheduler.schedule(() -> {
            if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                closeSub(subRef);
                return;
            }
            if (subRef.get() != null && s.getState() != DrainSession.State.A_MKT_BUY_DONE) {
                autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY", "A-BUY-WS");
                closeSub(subRef);
            }
        }, 3, TimeUnit.SECONDS);

        String orderId = null;
        try {
            if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                closeSub(subRef);
                return;
            }
            orderId = mexcRestFacade.limitBuyAboveSpreadA(symbol, usdtAmount, chatId, buyAClientId);
        } catch (Exception ex) {
            log.error("A BUY failed: {}", ex.getMessage(), ex);
        }
        if (orderId == null) {
            autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN,
                    "Не получилось купить по рынку с аккаунта А. Попробуйте уменьшить количество тиков для агрессивной покупки лимитки над спредом",
                    "A-BUY-REST");
            closeSub(subRef);
            return;
        }
        log.info("A BUY placed: symbol={} clientId={} orderId={}", symbol, buyAClientId, orderId);
    }

    /**
     * Один полный цикл (нижняя нога → верхняя нога).
     */
    private BigDecimal executeCycle(Long chatId, DrainSession s) {
        if (stopped(chatId, s)) {
            log.warn("⏹ executeCycle: stopped (state={}, flag={})", s.getState(), MemoryDb.getFlag(chatId).get());
            return BigDecimal.ZERO;
        }
        final long runIdLocal = s.getRunId();
        final String symbol = s.getSymbol();
        final SymbolFilters f = mexcRestFacade.getSymbolFilters(symbol);

        final Map<BigDecimal, BigDecimal> myAsks = new ConcurrentHashMap<>();
        final Map<BigDecimal, BigDecimal> myBids = new ConcurrentHashMap<>();

        // === нижняя кромка: SELL[A] ===
        BigDecimal pSell = mexcWsFacade.getNearLowerSpreadPriceExcludingMine(symbol, myBids, Collections.emptyMap());
        BigDecimal effMinNotional = MarketMath.resolveMinNotional(symbol, f.getMinNotional());
        BigDecimal minQtyForSell = MarketMath.minQtyForNotional(pSell, f.getStepSize(), effMinNotional);

        if (s.getQtyA() == null || s.getQtyA().compareTo(minQtyForSell) < 0) {
            return autoPauseAndZero(chatId, s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                    "qtyA < minNotional для SELL @ " + fmt(pSell) + " (qtyA=" + fmt(s.getQtyA()) + ", min=" + fmt(minQtyForSell) + ")",
                    "PRE-A-SELL-MIN");
        }

        log.info("[SELL_PLANNED] {} nearSell={} planQtyA={}", symbol, fmt(pSell), fmt(s.getQtyA()));

        String sellClientId = UUID.randomUUID().toString().replace("-", "");
        s.setSellOrderId(sellClientId);
        s.setPSell(pSell);
        s.setState(DrainSession.State.A_SELL_PLACED);
        // ВАЖНО: верификацию S2 делаем ТОЛЬКО после OrderAccepted (см. ниже)

        var subRef = new AtomicReference<OrderEventBus.Subscription>();
        OrderEventBus.Subscription sub = orderEventBus.subscribe(ev -> {
            if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                closeSub(subRef);
                return;
            }

            if (ev instanceof OrderEvent.OrderAccepted a) {
                // SELL[A] принят — проверяем AFTER_A_SELL_PLACED (с фазовой задержкой)
                verifyLater(chatId, s, BalanceControllerWs.Phase.AFTER_A_SELL_PLACED);

                BigDecimal qtyPlanned = MarketMath.normalizeQty(s.getQtyA(), f);
                String buyBClientId = UUID.randomUUID().toString().replace("-", "");

                myAsks.clear();
                myAsks.put(s.getPSell(), qtyPlanned);

                var subBRef = new AtomicReference<OrderEventBus.Subscription>();
                OrderEventBus.Subscription subB = orderEventBus.subscribe(evB -> {
                    if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                        closeSub(subBRef);
                        return;
                    }

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
                        closeSub(subBRef);

                    } else if (evB instanceof OrderEvent.OrderCanceled cb) {
                        BigDecimal filled = cb.cumulativeQty() == null ? BigDecimal.ZERO : cb.cumulativeQty();
                        if (filled.compareTo(qtyPlanned) < 0) {
                            autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.FRONT_RUN,
                                    "BUY[B] filled=" + fmt(filled) + " < planned=" + fmt(qtyPlanned), "B-BUY-IOC");
                        }
                        closeSub(subBRef);

                    } else if (evB instanceof OrderEvent.OrderRejected rb) {
                        autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "B BUY rejected: " + rb.reason(), "B-BUY-REJ");
                        closeSub(subBRef);
                    }
                }, OrderEventBus.byClientId(buyBClientId));
                subBRef.set(subB);

                drainScheduler.schedule(() -> {
                    if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                        closeSub(subBRef);
                        return;
                    }
                    if (subBRef.get() != null && s.getState() != DrainSession.State.AUTO_PAUSE) {
                        autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.TIMEOUT, "No WS status for B BUY", "B-BUY-WS");
                        closeSub(subBRef);
                    }
                }, 3, TimeUnit.SECONDS);

                try {
                    if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                        closeSub(subBRef);
                        return;
                    }
                    String oidB = mexcRestFacade.limitBuyAboveSpreadB(symbol, s.getPSell(), qtyPlanned, chatId, buyBClientId);
                    if (oidB == null) {
                        autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "B BUY REST failed/null orderId", "B-BUY-REST");
                        closeSub(subBRef);
                    } else {
                        s.setState(DrainSession.State.B_MKT_BUY_SENT);
                        log.info("➡️ {} BUY[B] placed: cidB={} orderIdB={} qtyPlanned={}", symbol, buyBClientId, oidB, fmt(qtyPlanned));
                    }
                } catch (Exception ex) {
                    log.error("BUY[B] send failed: {}", ex.getMessage(), ex);
                    autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "B MARKET-LIKE BUY send failed", "B-BUY-EX");
                    closeSub(subBRef);
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
                s.setLastCummA(fEv.cumulativeQuote());
                s.setLastFilledLowerQty(fEv.cumulativeQty());
                verifyLater(chatId, s, BalanceControllerWs.Phase.AFTER_LOWER_FILLED);

                closeSub(subRef);

                try {
                    BigDecimal pBuyUpper = mexcWsFacade.getNearUpperSpreadPriceExcludingMine(
                            symbol, Collections.emptyMap(), myAsks);
                    s.setPBuy(pBuyUpper);
                    log.info("[BUY_UPPER_PLANNED] {} nearBuy={} (excl my SELL[A] @ {})",
                            symbol, fmt(pBuyUpper), fmt(s.getPSell()));

                    BigDecimal qtyUpper = MarketMath.normalizeQty(nvl(s.getLastFilledLowerQty()), f);

                    BigDecimal effMinNotional2 = MarketMath.resolveMinNotional(symbol, f.getMinNotional());
                    BigDecimal minQtyNeedUpper = MarketMath.minQtyForNotional(pBuyUpper, f.getStepSize(), effMinNotional2);
                    if (qtyUpper.compareTo(minQtyNeedUpper) < 0 || qtyUpper.compareTo(f.getMinQty()) < 0) {
                        autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                                "qtyUpper < minNotional для BUY[A] @ " + fmt(pBuyUpper) +
                                        " (qtyUpper=" + fmt(qtyUpper) + ", min=" + fmt(minQtyNeedUpper) + ")",
                                "A-BUY-LOWER-MIN");
                        return;
                    }

                    String buyAUpperClientId = UUID.randomUUID().toString().replace("-", "");
                    var subAUpperRef = new AtomicReference<OrderEventBus.Subscription>();
                    OrderEventBus.Subscription subAUpper = orderEventBus.subscribe(evA -> {
                        if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                            closeSub(subAUpperRef);
                            return;
                        }

                        if (evA instanceof OrderEvent.OrderAccepted acc) {
                            myBids.clear();
                            myBids.put(s.getPBuy(), qtyUpper);
                            verifyLater(chatId, s, BalanceControllerWs.Phase.AFTER_A_BUY_PLACED);

                            String sellBBelowClientId = UUID.randomUUID().toString().replace("-", "");
                            var subSellBRef = new AtomicReference<OrderEventBus.Subscription>();
                            OrderEventBus.Subscription subSellB = orderEventBus.subscribe(evB2 -> {
                                if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                                    closeSub(subSellBRef);
                                    return;
                                }
                                if (evB2 instanceof OrderEvent.OrderPartiallyFilled pp) {
                                    log.info("📥 {} SELL[B] PARTIAL cid={} cumQty={} avg={} quote={}",
                                            symbol, pp.clientId(),
                                            pp.cumulativeQty().stripTrailingZeros(),
                                            pp.avgPrice().stripTrailingZeros(),
                                            pp.cumulativeQuote().stripTrailingZeros());
                                } else if (evB2 instanceof OrderEvent.OrderFilled ff) {
                                    log.info("✅ {} SELL[B] FILLED cid={} cumQty={} avg={} quote={} (taker fee incl.)",
                                            symbol, ff.clientId(),
                                            ff.cumulativeQty().stripTrailingZeros(),
                                            ff.avgPrice().stripTrailingZeros(),
                                            ff.cumulativeQuote().stripTrailingZeros());
                                    closeSub(subSellBRef);
                                } else if (evB2 instanceof OrderEvent.OrderCanceled cc) {
                                    log.warn("❗ {} SELL[B] IOC canceled (cum={})", symbol, fmt(cc.cumulativeQty()));
                                    closeSub(subSellBRef);
                                } else if (evB2 instanceof OrderEvent.OrderRejected rr) {
                                    autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "B SELL rejected: " + rr.reason(), "B-SELL-REJ");
                                    closeSub(subSellBRef);
                                }
                            }, OrderEventBus.byClientId(sellBBelowClientId));
                            subSellBRef.set(subSellB);

                            drainScheduler.schedule(() -> {
                                if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                                    closeSub(subSellBRef);
                                    return;
                                }
                                if (subSellBRef.get() != null && s.getState() != DrainSession.State.AUTO_PAUSE) {
                                    autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.TIMEOUT, "No WS status for B SELL", "B-SELL-WS");
                                    closeSub(subSellBRef);
                                }
                            }, 3, TimeUnit.SECONDS);

                            try {
                                if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                                    closeSub(subSellBRef);
                                    return;
                                }
                                String oidSellB = mexcRestFacade.limitSellBelowSpreadB(symbol, s.getPBuy(), qtyUpper, chatId, sellBBelowClientId);
                                if (oidSellB == null) {
                                    autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "B SELL REST failed/null orderId", "B-SELL-REST");
                                    closeSub(subSellBRef);
                                } else {
                                    log.info("➡️ {} SELL[B] placed: cidB={} orderIdB={} qty={}",
                                            symbol, sellBBelowClientId, oidSellB, fmt(qtyUpper));
                                }
                            } catch (Exception ex) {
                                log.error("SELL[B] send failed: {}", ex.getMessage(), ex);
                                autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "B MARKET-LIKE SELL send failed", "B-SELL-EX");
                                closeSub(subSellBRef);
                            }

                        } else if (evA instanceof OrderEvent.OrderFilled fA) {
                            log.info("✅ {} BUY[A] FILLED cid={} cumQty={} avg={} quote={}",
                                    symbol, fA.clientId(),
                                    fA.cumulativeQty().stripTrailingZeros(),
                                    fA.avgPrice().stripTrailingZeros(),
                                    fA.cumulativeQuote().stripTrailingZeros());

                            myBids.clear();

                            s.setQtyA(fA.cumulativeQty());
                            s.setLastSpentAUpper(fA.cumulativeQuote());
                            s.setLastFilledUpperQty(fA.cumulativeQty());

                            // продолжение — только после успешной верификации S5
                            verifyLaterAndMaybeContinue(chatId, s, f, myBids);

                            closeSub(subAUpperRef);

                        } else if (evA instanceof OrderEvent.OrderCanceled cA) {
                            myBids.clear();
                            log.warn("❗ {} BUY[A] canceled (cum={})", symbol, fmt(cA.cumulativeQty()));
                            closeSub(subAUpperRef);
                            autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.PARTIAL_MISMATCH, "A BUY@upper canceled", "A-BUY-CANCEL");

                        } else if (evA instanceof OrderEvent.OrderRejected rA) {
                            myBids.clear();
                            autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "A BUY rejected: " + rA.reason(), "A-BUY-REJ");
                            closeSub(subAUpperRef);
                        }
                    }, OrderEventBus.byClientId(buyAUpperClientId));
                    subAUpperRef.set(subAUpper);

                    drainScheduler.schedule(() -> {
                        if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                            closeSub(subAUpperRef);
                            return;
                        }
                        if (subAUpperRef.get() != null && s.getState() != DrainSession.State.AUTO_PAUSE) {
                            autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY@upper", "A-BUY-WS2");
                            closeSub(subAUpperRef);
                        }
                    }, 3, TimeUnit.SECONDS);

                    if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                        closeSub(subAUpperRef);
                        return;
                    }
                    mexcRestFacade.placeLimitBuyAAt(symbol, pBuyUpper, qtyUpper, chatId, buyAUpperClientId);

                } catch (Exception ex) {
                    log.warn("[BUY_UPPER_PLANNED/X] {} failed: {}", symbol, ex.toString());
                }

            } else if (ev instanceof OrderEvent.OrderCanceled c) {
                autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                        "SELL canceled (cum=" + fmt(c.cumulativeQty()) + ")", "A-SELL-CANCEL");
                closeSub(subRef);

            } else if (ev instanceof OrderEvent.OrderRejected r) {
                autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.UNKNOWN, "A SELL rejected: " + r.reason(), "A-SELL-REJ");
                closeSub(subRef);
            }
        }, OrderEventBus.byClientId(sellClientId));
        subRef.set(sub);

        drainScheduler.schedule(() -> {
            if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
                closeSub(subRef);
                return;
            }
            if (subRef.get() != null && s.getState() != DrainSession.State.A_SELL_FILLED && s.getState() != DrainSession.State.AUTO_PAUSE) {
                autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A SELL", "A-SELL-WS");
                closeSub(subRef);
            }
        }, 3, TimeUnit.SECONDS);

        if (s.getRunId() != runIdLocal || stopped(chatId, s)) {
            closeSub(subRef);
            return BigDecimal.ZERO;
        }
        mexcRestFacade.placeLimitSellA(s.getSymbol(), pSell, s.getQtyA(), chatId, sellClientId);
        return null;
    }

    // === /continue из фактических остатков ===
    public void continueFromBalances(String symbol, Long chatId) {
        if (symbol == null || symbol.isBlank()) {
            telegram.reply(chatId, "❌ /continue: не указан символ");
            return;
        }
        symbol = symbol.toUpperCase();

        DrainSession cur = MemoryDb.getSession(chatId);
        if (cur == null) {
            telegram.reply(chatId, "❌ /continue: активная сессия не найдена");
            return;
        }

        final String symFinal = symbol;
        final AtomicReference<String> err = new AtomicReference<>(null);

        MemoryDb.withSession(chatId, s -> {
            if (s.getState() != DrainSession.State.AUTO_PAUSE || MemoryDb.getFlag(chatId).get()) {
                err.set("❌ /continue: допускается только из состояния AUTO_PAUSE или после ручной паузы");
                return;
            }
            MemoryDb.getFlag(chatId).set(true);
            s.setRunId(s.getRunId() + 1); // новая эпоха «ручного» продолжения

            if (s.getSymbol() == null || !s.getSymbol().equalsIgnoreCase(symFinal)) {
                s.setSymbol(symFinal);
                orderBooks.startTracking(symFinal);
                log.info("🧱 L2 initialized for {}", symFinal);
            }

            final String base = baseAsset(s.getSymbol());
            final SymbolFilters f = mexcRestFacade.getSymbolFilters(s.getSymbol());

            BigDecimal aFreeBase = s.getABaseFree() == null ? BigDecimal.ZERO : s.getABaseFree();
            BigDecimal bBaseTot = s.bBaseTotal();

            BigDecimal dust = f.getStepSize();
            if (bBaseTot != null && bBaseTot.compareTo(dust) > 0) {
                err.set("❌ /continue: сначала доведи B." + base + " до нуля (сейчас: " + fmt(bBaseTot) + ")");
                return;
            }

            BigDecimal pSell = mexcWsFacade.getNearLowerSpreadPriceExcludingMine(
                    s.getSymbol(), Collections.emptyMap(), Collections.emptyMap());
            BigDecimal effMinNotional = MarketMath.resolveMinNotional(s.getSymbol(), f.getMinNotional());
            BigDecimal minQtyForSell = MarketMath.minQtyForNotional(pSell, f.getStepSize(), effMinNotional);

            if (aFreeBase == null || aFreeBase.compareTo(minQtyForSell) < 0) {
                err.set(("❌ /continue: на A недостаточно %s для следующего SELL @ %s. Нужно ≥ %s, есть %s")
                        .formatted(base, fmt(pSell), fmt(minQtyForSell), fmt(aFreeBase)));
                return;
            }

            BigDecimal qtyA = MarketMath.normalizeQty(aFreeBase, f);
            if (qtyA.compareTo(minQtyForSell) < 0) {
                err.set(("❌ /continue: после нормализации под шаг меньше минимума. Нужно ≥ %s, есть %s")
                        .formatted(fmt(minQtyForSell), fmt(qtyA)));
                return;
            }

            s.setQtyA(qtyA);
            s.setReason(null);
            s.setReasonDetails(null);
            s.setState(DrainSession.State.IDLE);
            s.setPSell(null);
            s.setPBuy(null);

            log.info("▶️ CONTINUE prepared: symbol={} base={} qtyA={} (minQtyForSell={})",
                    s.getSymbol(), base, fmt(qtyA), fmt(minQtyForSell));
        });

        if (err.get() != null) {
            telegram.reply(chatId, err.get());
            return;
        }

        DrainSession sNow = MemoryDb.getSession(chatId);
        if (sNow == null) {
            telegram.reply(chatId, "❌ /continue: сессия потеряна");
            return;
        }

        telegram.reply(chatId, "▶️ Продолжаю из фактического остатка на A: qtyA=" + fmt(sNow.getQtyA()) + " " + baseAsset(sNow.getSymbol()));
        executeCycle(chatId, sNow);
    }

    public void manualStop(Long chatId) {
        DrainSession s = MemoryDb.getSession(chatId);
        if (s == null) {
            telegram.reply(chatId, "ℹ️ /stop: активной сессии нет");
            return;
        }
        MemoryDb.getFlag(chatId).set(false);
        MemoryDb.withSession(chatId, ss -> ss.autoPause(DrainSession.AutoPauseReason.MANUAL, "Stopped by user"));
        log.warn("⏹ MANUAL STOP: {}", snapshot(MemoryDb.getSession(chatId)));
        telegram.reply(chatId, "⏸ Перелив поставлен на паузу (MANUAL).");
    }

    public String status(Long chatId) {
        if (MemoryDb.getSession(chatId) == null) return "Статус: нет активной сессии (IDLE)";
        AtomicReference<String> out = new AtomicReference<>("Статус недоступен");
        MemoryDb.withSession(chatId, s -> {
            String reason = (s.getReason() == null) ? "-" :
                    s.getReason() + (s.getReasonDetails() != null ? (" (" + s.getReasonDetails() + ")") : "");
            BigDecimal target = nz(s.getTargetDrainUSDT());
            BigDecimal drained = nz(s.getDrainedUSDT());
            String msg = """
                    📊 Статус перелива
                    Символ: %s
                    Состояние: %s
                    Причина/детали: %s
                    Цикл: #%d
                    Перелито: %s / %s USDT
                    Последние цены: SELL(lower)=%s  BUY(upper)=%s
                    A: base=%s  usdt=%s
                    B: base=%s  usdt=%s
                    Перестановки: sell=%d  buy=%d
                    Обновлено (ms): %d
                    """.formatted(
                    nzStr(s.getSymbol(), "-"),
                    s.getState(),
                    reason,
                    s.getCycleIndex(),
                    fmt(drained), fmt(target),
                    fmt(s.getPSell()), fmt(s.getPBuy()),
                    fmt(s.aBaseTotal()), fmt(s.aUsdtTotal()),
                    fmt(s.bBaseTotal()), fmt(s.bUsdtTotal()),
                    s.getRequotesSell(), s.getRequotesBuy(),
                    s.getTLastUpdate()
            );
            out.set(msg);
        });
        return out.get();
    }

    /**
     * Решение о продолжении/стопе. Вызывается ТОЛЬКО после успешной S5-верификации.
     */
    private void continueOrFinish(Long chatId, DrainSession s, SymbolFilters f, Map<BigDecimal, BigDecimal> myBids) {
        if (nvl(s.getDrainedUSDT()).compareTo(nvl(s.getTargetDrainUSDT())) >= 0) {
            autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.MANUAL,
                    "goal reached: drained " + fmt(s.getDrainedUSDT()) + " ≥ target " + fmt(s.getTargetDrainUSDT()), "GOAL");
            log.info("🏁 DONE: {}", snapshot(s));
            return;
        }

        long t0 = System.currentTimeMillis();
        BigDecimal lastBuyPx = s.getPBuy();
        while (System.currentTimeMillis() - t0 < GHOST_TTL_MS) {
            BigDecimal bb = orderBooks.bestBid(s.getSymbol());
            if (bb == null || lastBuyPx == null || bb.compareTo(lastBuyPx) != 0) break;
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        BigDecimal nextPSell = mexcWsFacade.getNearLowerSpreadPriceExcludingMine(
                s.getSymbol(), (myBids == null ? Collections.emptyMap() : myBids), Collections.emptyMap());
        BigDecimal effMinNotional = MarketMath.resolveMinNotional(s.getSymbol(), f.getMinNotional());
        BigDecimal minQtyNextSell = MarketMath.minQtyForNotional(nextPSell, f.getStepSize(), effMinNotional);

        if (s.getQtyA() == null || s.getQtyA().compareTo(minQtyNextSell) < 0) {
            autoPauseAndNotify(chatId, s, DrainSession.AutoPauseReason.INSUFFICIENT_BALANCE,
                    "next qtyA < minNotional для SELL @ " + fmt(nextPSell) +
                            " (qtyA=" + fmt(s.getQtyA()) + ", min=" + fmt(minQtyNextSell) + ")",
                    "NEXT-SELL-MIN");
            log.info("⛔ STOP (minNotional): {}", snapshot(s));
            return;
        }
        DrainSession fresh = MemoryDb.getSession(chatId);

        boolean stopRequested = fresh == null
                || fresh.getState() == DrainSession.State.AUTO_PAUSE
                || !MemoryDb.getFlag(chatId).get();
        if (stopRequested) {
            log.info("⏹ stop requested before next cycle (state={}, flag={}) — skip",
                    fresh == null ? null : fresh.getState(),
                    MemoryDb.getFlag(chatId).get());
            return;
        }

        log.info("🔄 CONTINUE: next pSell={} with qtyA={}", fmt(nextPSell), fmt(s.getQtyA()));
        executeCycle(chatId, s);
    }

    private void verifyLaterAndMaybeContinue(Long chatId, DrainSession s, SymbolFilters f, Map<BigDecimal, BigDecimal> myBids) {
        verifyLater(chatId, s, BalanceControllerWs.Phase.AFTER_UPPER_FILLED);
        drainScheduler.schedule(() -> {
            if (!stopped(chatId, s) && s.getState() != DrainSession.State.AUTO_PAUSE) {
                continueOrFinish(chatId, s, f, myBids);
            }
        }, 250, TimeUnit.MILLISECONDS);
    }

    private static BigDecimal nvl(BigDecimal x) {
        return x == null ? BigDecimal.ZERO : x;
    }

    private static String nzStr(String s, String def) {
        return s == null ? def : s;
    }

    private static BigDecimal nz(BigDecimal x) {
        return x == null ? BigDecimal.ZERO : x;
    }

    private static String baseAsset(String symbol) {
        if (symbol == null) return "";
        symbol = symbol.toUpperCase();
        if (symbol.endsWith("USDT")) return symbol.substring(0, symbol.length() - 4);
        return symbol;
    }

    private boolean stopped(Long chatId, DrainSession s) {
        return s.getState() == DrainSession.State.AUTO_PAUSE || !MemoryDb.getFlag(chatId).get();
    }

    private void closeSub(AtomicReference<OrderEventBus.Subscription> ref) {
        OrderEventBus.Subscription old = ref.getAndSet(null);
        if (old != null) old.close();
    }

    /* =====================  Автопауза / верификация  ===================== */

    private BigDecimal autoPauseAndZero(Long chatId, DrainSession s,
                                        DrainSession.AutoPauseReason reason,
                                        String details,
                                        String whereTag) {
        autoPauseAndNotify(chatId, s, reason, details, whereTag);
        return BigDecimal.ZERO;
    }

    // фазовые задержки для verify (мс)
    private static final Map<BalanceControllerWs.Phase, Long> PHASE_DELAY_MS = Map.of(
            BalanceControllerWs.Phase.AFTER_A_MKT_BUY, 250L,
            BalanceControllerWs.Phase.AFTER_A_SELL_PLACED, 1100L, // ждём фриз базы на A
            BalanceControllerWs.Phase.AFTER_LOWER_FILLED, 250L,
            BalanceControllerWs.Phase.AFTER_A_BUY_PLACED, 1100L, // ждём фриз USDT на A
            BalanceControllerWs.Phase.AFTER_UPPER_FILLED, 250L
    );

    /**
     * Любой фейл verify — сразу полноценная автопауза с опусканием флага и уведомлением.
     */
    private void verifyLater(Long chatId, DrainSession s, BalanceControllerWs.Phase ph) {
        var f = mexcRestFacade.getSymbolFilters(s.getSymbol());
        long delay = PHASE_DELAY_MS.getOrDefault(ph, 400L);
        drainScheduler.schedule(() -> {
            boolean ok = balanceControllerWs.verify(s, ph, f);
            if (!ok) {
                autoPauseAndNotify(chatId, s,
                        s.getReason() != null ? s.getReason() : DrainSession.AutoPauseReason.BALANCE_MISMATCH,
                        s.getReasonDetails(),
                        "VERIFY-" + ph);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Централизованная автопауза + Telegram.
     */
    private void autoPauseAndNotify(Long chatId, DrainSession s,
                                    DrainSession.AutoPauseReason reason,
                                    String details,
                                    String whereTag) {
        MemoryDb.getFlag(chatId).set(false);
        if (s.getState() != DrainSession.State.AUTO_PAUSE) {
            s.autoPause(reason, details);
        }

        log.warn("⏸ AUTO_PAUSE@{} [{}] reason={} | details={} | {}",
                whereTag, s.getSymbol(), reason, details, snapshot(s));

        try {
            BalanceSnapshot start = startBalances.computeIfAbsent(chatId, k ->
                    new BalanceSnapshot(
                            s.aUsdtTotal(), s.bUsdtTotal(),
                            s.aBaseTotal(), s.bBaseTotal(),
                            baseAsset(s.getSymbol())
                    )
            );

            BalanceSnapshot end = new BalanceSnapshot(
                    s.aUsdtTotal(), s.bUsdtTotal(),
                    s.aBaseTotal(), s.bBaseTotal(),
                    start.base()
            );

            BigDecimal drained = nvl(s.getDrainedUSDT());
            BigDecimal target = nvl(s.getTargetDrainUSDT());
            String pct = (target.signum() > 0)
                    ? drained.multiply(BigDecimal.valueOf(100)).divide(target, 2, RoundingMode.DOWN)
                    .stripTrailingZeros().toPlainString() + "%"
                    : "—";

            String msg = """
                    ⏸ *Автопауза* (`%s`)
                    _%s_
                    
                    • Прогресс: *%s / %s USDT* (%s)
                    
                    • Аккаунт A:
                      USDT: %s → %s
                      %s:   %s → %s
                    • Аккаунт B:
                      USDT: %s → %s
                      %s:   %s → %s
                    """.formatted(
                    reason, (details == null ? "" : details),
                    fmt(drained), fmt(target), pct,

                    fmt(start.aUsdt()), fmt(end.aUsdt()),
                    start.base(), fmt(start.aBase()), fmt(end.aBase()),

                    fmt(start.bUsdt()), fmt(end.bUsdt()),
                    start.base(), fmt(start.bBase()), fmt(end.bBase())
            );

            telegram.reply(chatId, msg);
        } catch (Exception e) {
            log.error("TG notify (auto-pause) failed: {}", e.getMessage(), e);
        }
    }

    private String snapshot(DrainSession s) {
        if (s == null) return "{session=null}";
        return new StringBuilder(320)
                .append("{state=").append(s.getState())
                .append(", cycle=").append(s.getCycleIndex())
                .append(", symbol=").append(s.getSymbol())
                .append(", runId=").append(s.getRunId())
                .append(", qtyA=").append(fmt(s.getQtyA()))
                .append(", pSell=").append(fmt(s.getPSell()))
                .append(", pBuy=").append(fmt(s.getPBuy()))
                .append(", lastSpentB=").append(fmt(s.getLastSpentB()))
                .append(", lastCummA=").append(fmt(s.getLastCummA()))
                .append(", drainedUSDT=").append(fmt(s.getDrainedUSDT()))
                .append(", targetDrainUSDT=").append(fmt(s.getTargetDrainUSDT()))
                .append(", sellOrderId=").append(s.getSellOrderId())
                .append(", buyOrderId=").append(s.getBuyOrderId())
                .append(", aUsdtTot=").append(fmt(s.aUsdtTotal()))
                .append(", aBaseTot=").append(fmt(s.aBaseTotal()))
                .append(", bUsdtTot=").append(fmt(s.bUsdtTotal()))
                .append(", bBaseTot=").append(fmt(s.bBaseTotal()))
                .append(", reason=").append(s.getReason())
                .append(", details=").append(s.getReasonDetails())
                .append('}')
                .toString();
    }
}

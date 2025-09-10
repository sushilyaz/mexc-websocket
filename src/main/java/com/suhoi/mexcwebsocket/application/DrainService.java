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
import java.util.UUID;
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

    private BigDecimal executeCycle(Long chatId, DrainSession s) {
        // ШАГ 1 определяем нижнюю границу и выставляем лимитный ордер на этой цене + SAFE_GUARD
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

        // 2) SELL(A) → подписка по clientId
        String sellClientId = UUID.randomUUID().toString().replace("-", "");
        s.setSellOrderId(sellClientId);
        s.setPSell(pSell);
        s.setState(DrainSession.State.A_SELL_PLACED);
        var subRef = new AtomicReference<OrderEventBus.Subscription>();
        OrderEventBus.Subscription sub = orderEventBus.subscribe(ev -> {
            // фильтр уже стоит по clientId

            if (ev instanceof OrderEvent.OrderAccepted a) {
                // Ордер принят (= NEW). Сразу отправляем MARKET BUY на B по сумме pSell*qtyA
                BigDecimal quote = pSell.multiply(s.getQtyA()); // при желании *1.001 на комиссию
                String buyBClientId = UUID.randomUUID().toString().replace("-", "");

                try {
                    // пока закомментирую
//                    mexcRestFacade.marketBuyQuoteB(symbol, quote, chatId, buyBClientId);
                    s.setState(DrainSession.State.B_MKT_BUY_SENT);
                    log.info("➡️ {} MARKET BUY[B] sent: cidB={} quote={}", symbol, buyBClientId, fmt(quote));
                } catch (Exception ex) {
                    log.error("MARKET BUY[B] send failed: {}", ex.getMessage(), ex);
                    s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "B MARKET BUY send failed");
                    OrderEventBus.Subscription old = subRef.getAndSet(null);
                    if (old != null) old.close();
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

                // шаг завершён; в вызывающем коде переходи к A_BUY_PLACED (верхняя кромка) и B_MKT_SELL_SENT
                // возвращаемое значение можешь использовать как «сигнал» продолжения
                // здесь вернём null — ты итерацию контролируешь снаружи по state
                // return fEv.cumulativeQty();

            } else if (ev instanceof OrderEvent.OrderCanceled c) {
                // Для GTC SELL странно, но всякое бывает
                s.autoPause(DrainSession.AutoPauseReason.PARTIAL_MISMATCH,
                        "SELL canceled (cum="+fmt(c.cumulativeQty())+")");
                OrderEventBus.Subscription old = subRef.getAndSet(null);
                if (old != null) old.close();

            } else if (ev instanceof OrderEvent.OrderRejected r) {
                s.autoPause(DrainSession.AutoPauseReason.UNKNOWN, "A SELL rejected: " + r.reason());
                OrderEventBus.Subscription old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, OrderEventBus.byClientId(sellClientId));
        subRef.set(sub);

        // 3) (необязательный) timeout-guard, чтобы не повиснуть навсегда
        drainScheduler.schedule(() -> {
            if (subRef.get() != null && s.getState() != DrainSession.State.A_SELL_FILLED && s.getState() != DrainSession.State.AUTO_PAUSE) {
                s.autoPause(DrainSession.AutoPauseReason.TIMEOUT, "No WS status for A BUY");
                var old = subRef.getAndSet(null);
                if (old != null) old.close();
            }
        }, 3, TimeUnit.SECONDS);

        mexcRestFacade.placeLimitSellA(s.getSymbol(), pSell, s.getQtyA(), chatId, sellClientId);
        // далее эта лимитка как появится в стакане (это надо увидеть через вебсокет) - сразу же выкупаем ее по рынку с аккаунта Б
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

package com.suhoi.mexcwebsocket.mexc.ws.market;

import com.suhoi.mexcwebsocket.domain.model.L1;
import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import com.suhoi.mexcwebsocket.infra.OrderEventBus;
import com.suhoi.mexcwebsocket.mexc.rest.MexcRestFacade;
import com.suhoi.mexcwebsocket.util.MarketMath;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
@Slf4j
public class MexcWsFacade {
    private final OrderBookService orderBookService;
    private final OrderEventBus orderEventBus;
    private final MexcRestFacade mexcRestFacade;

    private static final BigDecimal SPREAD_GUARD = new BigDecimal("0.10"); // как у тебя

    private static boolean notMultiple(BigDecimal px, BigDecimal step) {
        return px != null && step != null && step.signum() > 0
                && px.remainder(step).compareTo(BigDecimal.ZERO) != 0;
    }

    private static int safeScale(BigDecimal v) {
        return (v == null) ? 0 : v.scale(); // не stripTrailingZeros — нам важна исходная точность
    }

    public BigDecimal getNearLowerSpreadPrice(String symbol) {
        final String s = symbol.toUpperCase();

        // 1) фильтры (как есть)
        SymbolFilters f = mexcRestFacade.getSymbolFilters(s);
        BigDecimal tick = f.getTickSize();
        Integer qp = f.getQuotePrecision();

        // 2) L1
        L1 l1 = orderBookService.getSnapshotL1(s);
        BigDecimal bid = (l1 != null) ? l1.getBid() : null;
        BigDecimal ask = (l1 != null) ? l1.getAsk() : null;

        // 3) фоллбэк на случай пустого L1
        if (bid == null || bid.signum() <= 0) {
            BigDecimal fb = MarketMath.normalizePrice(tick, tick);
            log.info("[nearLower:{}] Fallback: bid={} tick={} qp={} -> {}",
                    s, bid, tick, qp, fb);
            return fb;
        }

        // 4) КРИТИЧЕСКОЕ: если тик из фильтров не делит L1 — берём тик из масштаба L1
        if (tick == null || tick.signum() <= 0 || notMultiple(bid, tick) || (ask != null && notMultiple(ask, tick))) {
            int scale = Math.max(safeScale(bid), safeScale(ask));
            BigDecimal tickByL1 = (scale > 0) ? BigDecimal.ONE.movePointLeft(scale) : tick;
            log.warn("[nearLower:{}] tick from filters {} (qp={}) не делит L1 (bid={} ask={}) -> override tick={}",
                    s,
                    (tick == null ? "null" : tick.toPlainString()),
                    qp,
                    bid.toPlainString(),
                    (ask == null ? "null" : ask.toPlainString()),
                    (tickByL1 == null ? "null" : tickByL1.toPlainString()));
            tick = tickByL1;
        }

        // 5) Просто: bestBid на сетку вниз + 1 тик
        BigDecimal alignedBid = MarketMath.floorToStep(bid, tick);
        BigDecimal nextAboveBid = alignedBid.add(tick);

        log.info("[nearLower:{}] SIMPLE: bid={} tick={} | alignedBid={} -> nextAboveBid={}",
                s,
                bid.stripTrailingZeros().toPlainString(),
                tick.stripTrailingZeros().toPlainString(),
                alignedBid.stripTrailingZeros().toPlainString(),
                nextAboveBid.stripTrailingZeros().toPlainString()
        );

        return nextAboveBid.stripTrailingZeros();
    }




}


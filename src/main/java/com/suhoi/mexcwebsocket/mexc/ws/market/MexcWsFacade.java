package com.suhoi.mexcwebsocket.mexc.ws.market;

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

    // где-нибудь в MexcRestFacade / TradePlanner (где у тебя уже есть доступ к orderBooks и фильтрам)
    private static final BigDecimal SPREAD_GUARD = new BigDecimal("0.10"); // 25% от спреда вверх от bid
    private static final int MIN_TICKS_FROM_BID = 1;       // минимум на 1 тик выше bid
    private static final int MIN_TICKS_BELOW_ASK = 1;      // и минимум на 1 тик ниже ask

    /** Цена для SELL "возле нижней кромки" (внутри спреда) по локальному стакану. */
    /** Цена для SELL "возле нижней кромки" (внутри спреда) по локальному стакану. */
    public BigDecimal getNearLowerSpreadPrice(String symbol) {
        final String s = symbol.toUpperCase();

        var f = mexcRestFacade.getSymbolFilters(s);
        var tick = f.getTickSize();

        var l1   = orderBookService.getSnapshotL1(s);
        BigDecimal bid = (l1 != null) ? l1.getBid() : null;
        BigDecimal ask = (l1 != null) ? l1.getAsk() : null;

        // Фолбэк, если стакан пуст/битый
        if (bid == null || bid.signum() <= 0 || ask == null || ask.signum() <= 0) {
            BigDecimal p = (tick != null && tick.signum() > 0) ? tick : new BigDecimal("0.00000001");
            return p.stripTrailingZeros();
        }

        BigDecimal spread = ask.subtract(bid);
        if (spread.signum() <= 0) {
            // пересечённый/нулевой спред → просто на тик выше bid, но ниже ask
            BigDecimal p = MarketMath.alignPriceCeil(bid.add(tick), tick);
            if (p.compareTo(ask) >= 0) {
                p = MarketMath.floorToStep(ask.subtract(tick), tick);
            }
            return (p.signum() > 0 ? p : tick).stripTrailingZeros();
        }

        // Сырая точка внутри спреда ближе к bid
        BigDecimal insideRaw = bid.add(spread.multiply(SPREAD_GUARD));

        // Зажим: строго внутри (bid + 1*tick .. ask - 1*tick)
        BigDecimal minInside = bid.add(tick.multiply(BigDecimal.valueOf(MIN_TICKS_FROM_BID)));
        BigDecimal maxInside = ask.subtract(tick.multiply(BigDecimal.valueOf(MIN_TICKS_BELOW_ASK)));
        if (maxInside.compareTo(minInside) < 0) maxInside = minInside;

        BigDecimal clamped = insideRaw.max(minInside).min(maxInside);

        // Для SELL — "ceil" к сетке, чтобы не сползти ниже расчётной точки
        BigDecimal p = MarketMath.alignPriceCeil(clamped, tick);

        // Жёсткие гарантии: строго внутри спреда
        if (p.compareTo(ask) >= 0) p = MarketMath.floorToStep(ask.subtract(tick), tick);
        if (p.compareTo(bid) <= 0) p = MarketMath.alignPriceCeil(bid.add(tick), tick);

        return (p.signum() > 0 ? p : tick).stripTrailingZeros();
    }

}

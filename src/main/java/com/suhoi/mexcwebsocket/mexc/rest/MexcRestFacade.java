package com.suhoi.mexcwebsocket.mexc.rest;

import com.suhoi.mexcwebsocket.db.Cache;
import com.suhoi.mexcwebsocket.db.MemoryDb;
import com.suhoi.mexcwebsocket.domain.model.CachedSymbolInfo;
import com.suhoi.mexcwebsocket.domain.model.Creds;
import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import com.suhoi.mexcwebsocket.infra.OrderEventBus;
import com.suhoi.mexcwebsocket.mapper.MexcMapper;
import com.suhoi.mexcwebsocket.mexc.rest.dto.response.SymbolInfoResponseDto;
import com.suhoi.mexcwebsocket.mexc.ws.market.OrderBookService;
import com.suhoi.mexcwebsocket.util.MarketMath;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
@RequiredArgsConstructor
@Slf4j
public class MexcRestFacade {

    private final MexcRestClient mexcRestClient;
    public static final long EXCHANGE_INFO_TTL_MS = 60_000L;
    private static final int TICK_ABOVE = 4;
    private final OrderBookService orderBookService;
    private final OrderEventBus orderEventBus;

    public SymbolFilters getSymbolFilters(String symbol) {
        CachedSymbolInfo cachedSymbolInfo = Cache.exchangeInfoCache.get(symbol);

        long now = System.currentTimeMillis();
        if (cachedSymbolInfo != null && (now - cachedSymbolInfo.getLoadedAt()) < EXCHANGE_INFO_TTL_MS) {
            return cachedSymbolInfo.getFilters();
        }

        SymbolInfoResponseDto symbolInformation = mexcRestClient.getSymbolInformation(symbol);
        return MexcMapper.mapToFilters(symbolInformation, symbol);
    }

    public String placeLimitSellA(String symbol, BigDecimal price, BigDecimal qty, Long chatId, String clientId) {
        Creds creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA");

        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal tick = f.getTickSize();

        BigDecimal p = MarketMath.alignPriceCeil(price, tick);
        BigDecimal q = MarketMath.normalizeQty(qty, f);

        BigDecimal effMinNotional = MarketMath.resolveMinNotional(symbol, f.getMinNotional());
        BigDecimal minQtyNeed = MarketMath.minQtyForNotional(p, f.getStepSize(), effMinNotional);
        if (q.compareTo(minQtyNeed) < 0 || q.compareTo(f.getMinQty()) < 0) {
            log.warn("SELL {}: qty {} не проходит minNotional/minQty", symbol, q);
            return null;
        }

        return mexcRestClient.newOrder(
                symbol, "SELL", "LIMIT", "GTC",
                q.toPlainString(), p.toPlainString(), clientId,
                creds.getApiKey(), creds.getSecret()
        );
    }


    public String limitBuyAboveSpreadA(String symbol, BigDecimal usdtAmount, Long chatId, String clientId) {
        Creds creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA (chatId=" + chatId + ")");

        SymbolFilters symbolFilters = getSymbolFilters(symbol);
        log.info("SymbolFilters: {}", symbolFilters);
        // необязательно в нашем случае
        BigDecimal minNotional = MarketMath.resolveMinNotional(symbol, symbolFilters.getMinNotional());

        log.info("MinNotional: {}", minNotional);
        // цена над спредом из локального стакана
        BigDecimal price = priceAboveAsk(symbol);

        // Бюджет → «сырое» qty → нормализация к stepSize
        if (usdtAmount == null || usdtAmount.signum() <= 0) {
            log.warn("LIMIT BUY[AGGR] {}: бюджет <= 0 ({}) — отмена", symbol, usdtAmount);
            return null;
        }

        BigDecimal rawQty = usdtAmount.divide(price, 18, RoundingMode.DOWN);
        BigDecimal qty = MarketMath.normalizeQty(rawQty, symbolFilters);

        // minNotional: сколько минимально нужно qty при этой price
        BigDecimal minQtyNeed = MarketMath.minQtyForNotional(price, symbolFilters.getStepSize(), minNotional);
        log.info("MinQtyNeed: {}", minQtyNeed);
        if (qty.compareTo(minQtyNeed) < 0) {
            BigDecimal needCost = minQtyNeed.multiply(price);
            if (needCost.compareTo(usdtAmount) <= 0) {
                qty = minQtyNeed;
            } else {
                log.warn("LIMIT BUY[AGGR] {}: бюджет {} USDT < minNotional {} (нужно {} USDT). Ордер НЕ отправлен.",
                        symbol,
                        usdtAmount.stripTrailingZeros().toPlainString(),
                        minNotional.stripTrailingZeros().toPlainString(),
                        needCost.stripTrailingZeros().toPlainString());
                return null;
            }
        }
        if (qty == null || qty.signum() <= 0) {
            log.warn("LIMIT BUY[AGGR] {}: qty<=0 после расчётов (budget={}, price={}, stepSize={})",
                    symbol, usdtAmount, price, symbolFilters.getStepSize());
            return null;
        }

        // отправляем ордер
        return mexcRestClient.newOrder(
                symbol, "BUY", "LIMIT","IOC", qty.toPlainString(), price.toPlainString(),
                clientId, creds.getApiKey(), creds.getSecret());
    }

    public String limitBuyAboveSpreadB(String symbol, BigDecimal usdtAmount, Long chatId, String clientId) {
        Creds creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        SymbolFilters symbolFilters = getSymbolFilters(symbol);
        log.info("SymbolFilters: {}", symbolFilters);
        // необязательно в нашем случае
        BigDecimal minNotional = MarketMath.resolveMinNotional(symbol, symbolFilters.getMinNotional());

        log.info("MinNotional: {}", minNotional);
        // цена над спредом из локального стакана
        BigDecimal price = priceAboveAsk(symbol);

        // Бюджет → «сырое» qty → нормализация к stepSize
        if (usdtAmount == null || usdtAmount.signum() <= 0) {
            log.warn("LIMIT BUY[AGGR] {}: бюджет <= 0 ({}) — отмена", symbol, usdtAmount);
            return null;
        }

        BigDecimal rawQty = usdtAmount.divide(price, 18, RoundingMode.DOWN);
        BigDecimal qty = MarketMath.normalizeQty(rawQty, symbolFilters);

        // minNotional: сколько минимально нужно qty при этой price
        BigDecimal minQtyNeed = MarketMath.minQtyForNotional(price, symbolFilters.getStepSize(), minNotional);
        log.info("MinQtyNeed: {}", minQtyNeed);
        if (qty.compareTo(minQtyNeed) < 0) {
            BigDecimal needCost = minQtyNeed.multiply(price);
            if (needCost.compareTo(usdtAmount) <= 0) {
                qty = minQtyNeed;
            } else {
                log.warn("LIMIT BUY[AGGR] {}: бюджет {} USDT < minNotional {} (нужно {} USDT). Ордер НЕ отправлен.",
                        symbol,
                        usdtAmount.stripTrailingZeros().toPlainString(),
                        minNotional.stripTrailingZeros().toPlainString(),
                        needCost.stripTrailingZeros().toPlainString());
                return null;
            }
        }
        if (qty == null || qty.signum() <= 0) {
            log.warn("LIMIT BUY[AGGR] {}: qty<=0 после расчётов (budget={}, price={}, stepSize={})",
                    symbol, usdtAmount, price, symbolFilters.getStepSize());
            return null;
        }

        // отправляем ордер
        return mexcRestClient.newOrder(
                symbol, "BUY", "LIMIT","IOC", qty.toPlainString(), price.toPlainString(),
                clientId, creds.getApiKey(), creds.getSecret());
    }

    /**
     * Цена НАД спредом (для BUY) на базе локального стакана:
     * ask + N * tickSize, с аккуратным floor к сетке и гарантиями: строго > ask.
     */
    private BigDecimal priceAboveAsk(String symbol) {
        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal tick = f.getTickSize();

        // читаем L1 из локального стакана
        var l1 = orderBookService.getSnapshotL1(symbol);
        BigDecimal ask = (l1 != null) ? l1.getAsk() : null;
        BigDecimal bid = (l1 != null) ? l1.getBid() : null;

        // если стакан не свежий — можно логнуть/подстраховаться (опционально)
        if (!orderBookService.isFresh(symbol)) {
            log.debug("L1 for {} is stale; tsAge={}ms", symbol,
                    (l1 == null ? -1 : (System.currentTimeMillis() - l1.getTs())));
        }

        // базовая цена: ask, иначе bid (зеркалим), иначе один тик
        BigDecimal base = (ask != null && ask.signum() > 0) ? ask
                : (bid != null && bid.signum() > 0) ? bid
                : (tick != null && tick.signum() > 0) ? tick
                : new BigDecimal("0.00000001");

        int n = Math.max(1, TICK_ABOVE);
        BigDecimal raw = base.add(tick.multiply(BigDecimal.valueOf(n)));

        // floor к сетке тика
        BigDecimal p = MarketMath.floorToStep(raw, tick);

        // гарантия, что действительно строго выше ask
        if (ask != null && ask.signum() > 0 && p.compareTo(ask) <= 0) {
            p = MarketMath.floorToStep(ask.add(tick), tick);
            if (p.compareTo(ask) <= 0) {
                // на всякий случай — если ask «не по сетке», добросим ещё тик
                p = MarketMath.floorToStep(ask.add(tick.multiply(BigDecimal.valueOf(2))), tick);
            }
        }

        // финальная нормализация/защита
        p = MarketMath.normalizePrice(p, tick);

        // (опционально) лог
        log.info("[PRICE_ABOVE_ASK] {} ask={} ticks={} tick={} -> price={}",
                symbol,
                ask == null ? "null" : ask.stripTrailingZeros().toPlainString(),
                n,
                tick.stripTrailingZeros().toPlainString(),
                p.stripTrailingZeros().toPlainString());

        return p;
    }
}

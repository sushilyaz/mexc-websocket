package com.suhoi.mexcwebsocket.mexc.rest;

import com.suhoi.mexcwebsocket.db.Cache;
import com.suhoi.mexcwebsocket.db.MemoryDb;
import com.suhoi.mexcwebsocket.domain.model.CachedSymbolInfo;
import com.suhoi.mexcwebsocket.domain.model.Creds;
import com.suhoi.mexcwebsocket.domain.model.L1;
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
    // комиссия тейкера 0.05% (для BUY в USDT-парах комиссия в USDT)
    public static final BigDecimal TAKER_FEE_B = new BigDecimal("0.0005");
    // на сколько тиков поднять лимитную цену BUY(B) над pSell (0 или 1 обычно достаточно)

    public SymbolFilters getSymbolFilters(String symbol) {
        CachedSymbolInfo cachedSymbolInfo = Cache.exchangeInfoCache.get(symbol);

        long now = System.currentTimeMillis();
        if (cachedSymbolInfo != null && (now - cachedSymbolInfo.getLoadedAt()) < EXCHANGE_INFO_TTL_MS) {
            return cachedSymbolInfo.getFilters();
        }

        SymbolInfoResponseDto symbolInformation = mexcRestClient.getSymbolInformation(symbol);
        log.info("[symbol:{}] qp={}", symbol, symbolInformation.getQuotePrecision());
        return MexcMapper.mapToFilters(symbolInformation, symbol);
    }

    // MexcRestFacade.java
    public void placeLimitSellA(String symbol, BigDecimal price, BigDecimal qty, Long chatId, String clientId) {
        Creds creds = MemoryDb.getAccountA(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountA");

        final String s = symbol.toUpperCase();

        SymbolFilters f = getSymbolFilters(s);
        BigDecimal declaredTick = f.getTickSize();
        Integer qp = f.getQuotePrecision();

        L1 l1 = orderBookService.getSnapshotL1(s);
        BigDecimal bid = (l1 != null) ? l1.getBid() : null;
        BigDecimal ask = (l1 != null) ? l1.getAsk() : null;

        // 1) Вычисляем ЭФФЕКТИВНЫЙ тик по L1/plan (если фильтры врут по точности)
        BigDecimal tickEff = effectiveTickForSell(s, price, bid, ask, f);

        // 2) НИКАКИХ clamp внутри спреда. Просто выравниваем вниз к сетке на всякий случай
        BigDecimal pPlanned = price;
        BigDecimal pSend = MarketMath.floorToStep(pPlanned, tickEff);

        // 3) Нормализуем количество и проверяем minNotional/minQty на ФАКТИЧЕСКОЙ цене отправки
        BigDecimal q = MarketMath.normalizeQty(qty, f);

        BigDecimal effMinNotional = MarketMath.resolveMinNotional(s, f.getMinNotional());
        BigDecimal minQtyNeed = MarketMath.minQtyForNotional(pSend, f.getStepSize(), effMinNotional);
        if (q.compareTo(minQtyNeed) < 0 || q.compareTo(f.getMinQty()) < 0) {
            log.warn("SELL[{}]: qty={} не проходит minNotional/minQty при p={}",
                    s,
                    q.stripTrailingZeros().toPlainString(),
                    pSend.stripTrailingZeros().toPlainString());
            return;
        }

        // 4) Лог — без «коридоров» и «клампов»
        log.info("SELL[{}] plan={} -> send={} tick={} (declared={}) qp={} | bid={} ask={}",
                s,
                pPlanned.stripTrailingZeros().toPlainString(),
                pSend.stripTrailingZeros().toPlainString(),
                tickEff.stripTrailingZeros().toPlainString(),
                declaredTick == null ? "null" : declaredTick.stripTrailingZeros().toPlainString(),
                qp,
                bid == null ? "null" : bid.stripTrailingZeros().toPlainString(),
                ask == null ? "null" : ask.stripTrailingZeros().toPlainString()
        );

        // 5) Отправка ордера
        mexcRestClient.newOrder(
                s, "SELL", "LIMIT", "GTC",
                q.toPlainString(), pSend.toPlainString(), clientId,
                creds.getApiKey(), creds.getSecret()
        );
    }

    /**
     * Возвращает "эффективный" тик для SELL:
     * - если declared tick (из filters) не делит plan/bid/ask — берём максимальный scale из них/qp
     * - иначе оставляем declared tick.
     */
    private BigDecimal effectiveTickForSell(String symbol,
                                            BigDecimal plan,
                                            BigDecimal bid,
                                            BigDecimal ask,
                                            SymbolFilters f) {
        BigDecimal tick = f.getTickSize();
        Integer qp = f.getQuotePrecision();

        boolean needOverride =
                tick == null || tick.signum() <= 0
                        || notMultiple(plan, tick)
                        || (bid != null && notMultiple(bid, tick))
                        || (ask != null && notMultiple(ask, tick));

        if (needOverride) {
            int scale = Math.max(
                    Math.max(safeScale(plan), Math.max(safeScale(bid), safeScale(ask))),
                    (qp != null ? qp : 0)
            );
            if (scale <= 0 && tick != null) scale = Math.max(scale, safeScale(tick));
            if (scale <= 0) scale = 6; // безопасный дефолт, сюда почти не попадём

            tick = BigDecimal.ONE.movePointLeft(scale);
            log.warn("[symbol:{}] SELL: override tick by L1/plan -> {}", symbol, tick.stripTrailingZeros().toPlainString());
        }
        return tick;
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

    public String limitBuyAboveSpreadB(String symbol,
                                       BigDecimal pSell,         // цена нашего SELL(A) у нижней кромки
                                       BigDecimal qtyWanted,     // хотим купить ровно столько же
                                       Long chatId,
                                       String clientId) {
        Creds creds = MemoryDb.getAccountB(chatId);
        if (creds == null) throw new IllegalArgumentException("Нет ключей для accountB (chatId=" + chatId + ")");

        SymbolFilters f = getSymbolFilters(symbol);
        BigDecimal tick = f.getTickSize();

        // лимитная цена, которая пересекает pSell (>= pSell). Обычно хватает pSell; при желании +tick.
        BigDecimal bump = tick.multiply(BigDecimal.valueOf(Math.max(0, TICK_ABOVE)));
        BigDecimal pCross = MarketMath.alignPriceCeil(pSell.add(bump), tick);

        // нормализуем количество
        BigDecimal q = MarketMath.normalizeQty(qtyWanted, f);

        // minNotional: проверяем на pCross (по правилам биржи)
        BigDecimal effMinNotional = MarketMath.resolveMinNotional(symbol, f.getMinNotional());
        BigDecimal minQtyNeed = MarketMath.minQtyForNotional(pCross, f.getStepSize(), effMinNotional);
        if (q.compareTo(minQtyNeed) < 0 || q.compareTo(f.getMinQty()) < 0) {
            log.warn("BUY[B][AGGR] {}: qty={} не проходит minNotional/minQty при pCross={}",
                    symbol, q.stripTrailingZeros(), pCross.stripTrailingZeros());
            return null;
        }

        // это только для логов/контроля: сколько USDT теоретически нужно и с комиссией
        BigDecimal quoteAtOurAsk = pSell.multiply(q); // исполняться будем по pSell (цена ордера A), а не по pCross
        BigDecimal quoteWithFee   = quoteAtOurAsk.multiply(BigDecimal.ONE.add(TAKER_FEE_B));
        // округлим только для лога (отправляем quantity/price, не quoteOrderQty)
        int qp = Math.max(0, f.getQuotePrecision());
        BigDecimal qLog = quoteAtOurAsk.setScale(qp, RoundingMode.DOWN).stripTrailingZeros();
        BigDecimal qFeeLog = quoteWithFee.setScale(qp, RoundingMode.DOWN).stripTrailingZeros();

        log.info("BUY[B][AGGR] plan: symbol={} pSell={} pCross={} qty={} quote~{} quote+fee~{}",
                symbol,
                pSell.stripTrailingZeros().toPlainString(),
                pCross.stripTrailingZeros().toPlainString(),
                q.stripTrailingZeros().toPlainString(),
                qLog.toPlainString(),
                qFeeLog.toPlainString());

        // Отправляем LIMIT IOC
        return mexcRestClient.newOrder(
                symbol, "BUY", "LIMIT", "IOC",
                q.toPlainString(), pCross.toPlainString(), clientId,
                creds.getApiKey(), creds.getSecret()
        );
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
    private static boolean notMultiple(BigDecimal px, BigDecimal step) {
        return px != null && step != null && step.signum() > 0
                && px.remainder(step).compareTo(BigDecimal.ZERO) != 0;
    }
    private static int safeScale(BigDecimal v) {
        return v == null ? 0 : v.scale();
    }
    private BigDecimal effectiveTick(String symbol,
                                     BigDecimal plan,
                                     BigDecimal bid,
                                     BigDecimal ask,
                                     SymbolFilters f) {
        BigDecimal tick = f.getTickSize();
        Integer qp = f.getQuotePrecision();

        if (tick == null || tick.signum() <= 0
                || notMultiple(plan, tick)
                || notMultiple(bid, tick)
                || notMultiple(ask, tick)) {

            int scale = Math.max(Math.max(safeScale(bid), safeScale(ask)), (qp != null ? qp : 0));
            if (scale > 0) tick = BigDecimal.ONE.movePointLeft(scale);
            // Лог — чтобы видеть, что именно починило ситуацию
            log.warn("[symbol:{}] SELL: override tick by L1/plan -> {}", symbol, tick.toPlainString());
        }
        return tick;
    }

}

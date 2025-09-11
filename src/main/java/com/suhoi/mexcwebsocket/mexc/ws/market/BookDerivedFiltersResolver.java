// src/main/java/com/suhoi/mexcwebsocket/mexc/ws/market/BookDerivedFiltersResolver.java
package com.suhoi.mexcwebsocket.mexc.ws.market;

import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class BookDerivedFiltersResolver {

    private final OrderBookService orderBooks;

    private static final int LEVELS = 50;     // сколько уровней берём для оценки шага
    private static final int MAX_SCALE = 18;  // максимум знаков после запятой
    private static final BigDecimal DEFAULT_MIN_NOTIONAL = new BigDecimal("1"); // безопасный дефолт

    /** Построить фильтры по текущему стакану. */
    public SymbolFilters derive(String symbol) {
        String sym = symbol.toUpperCase();

        // берём СНЕПШОТЫ (копии!) стакана
        NavigableMap<BigDecimal, BigDecimal> asksSnap = topNAscending(orderBooks.asksSnapshot(sym), LEVELS, true);
        NavigableMap<BigDecimal, BigDecimal> bidsSnapAsc = topNAscending(orderBooks.bidsSnapshot(sym), LEVELS, false);

        if (asksSnap.isEmpty() && bidsSnapAsc.isEmpty()) {
            log.warn("[{}] orderbook empty -> fallback defaults", sym);
            return new SymbolFilters(
                    new BigDecimal("0.00000001"), // tickSize
                    new BigDecimal("0.00000001"), // stepSize
                    BigDecimal.ZERO,              // minQty
                    DEFAULT_MIN_NOTIONAL,         // minNotional
                    8                             // quotePrecision
            );
        }

        // --- tickSize: НОД соседних разностей цен
        List<BigDecimal> diffs = new ArrayList<>();
        diffs.addAll(adjacentDiffs(asksSnap));
        diffs.addAll(adjacentDiffs(bidsSnapAsc)); // тут bidsSnapAsc уже по возрастанию

        BigDecimal tick = gcdForDecimals(diffs);
        if (isZeroOrNull(tick)) {
            tick = minPositive(diffs).orElseGet(() -> estimateTickFromOneSide(asksSnap, bidsSnapAsc));
        }
        tick = normalizeScale(tick);

        // --- stepSize: НОД количеств
        List<BigDecimal> qtys = new ArrayList<>(asksSnap.values());
        qtys.addAll(bidsSnapAsc.values());
        qtys = qtys.stream().filter(q -> q != null && q.signum() > 0).collect(Collectors.toList());

        BigDecimal step = gcdForDecimals(qtys);
        if (isZeroOrNull(step)) {
            int maxScale = qtys.stream().map(BigDecimal::scale).max(Integer::compareTo).orElse(0);
            maxScale = Math.min(maxScale, MAX_SCALE);
            step = BigDecimal.ONE.scaleByPowerOfTen(-maxScale);
        }
        step = normalizeScale(step);

        int quotePrecision = Math.max(0, tick.stripTrailingZeros().scale());

        SymbolFilters f = new SymbolFilters(
                tick,
                step,
                BigDecimal.ZERO,          // minQty оставим 0 — ты проверяешь minNotional
                DEFAULT_MIN_NOTIONAL,
                quotePrecision
        );

        log.info("[symbol:{}] Derived from book => {}", sym, f);
        return f;
    }

    // ===== helpers =====

    /** Гарантируем восходящий порядок и берём первые N уровней. */
    private static NavigableMap<BigDecimal, BigDecimal> topNAscending(NavigableMap<BigDecimal, BigDecimal> src, int n, boolean isAsks) {
        NavigableMap<BigDecimal, BigDecimal> out = new TreeMap<>();
        if (src == null || src.isEmpty()) return out;

        // source может быть и по убыванию (твои bidsSnapshot часто такие) — сделаем восходящую копию
        NavigableMap<BigDecimal, BigDecimal> asc = new TreeMap<>(src);
        for (var e : asc.entrySet()) {
            if (out.size() >= n) break;
            if (e.getKey() != null && e.getValue() != null && e.getValue().signum() > 0) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    private static List<BigDecimal> adjacentDiffs(NavigableMap<BigDecimal, BigDecimal> sideAsc) {
        List<BigDecimal> diffs = new ArrayList<>();
        if (sideAsc == null || sideAsc.size() < 2) return diffs;
        BigDecimal prev = null;
        for (BigDecimal p : sideAsc.keySet()) {
            if (prev != null) {
                BigDecimal d = p.subtract(prev);
                if (d.signum() > 0) diffs.add(d);
            }
            prev = p;
        }
        return diffs;
    }

    private static BigDecimal gcdForDecimals(List<BigDecimal> values) {
        if (values == null || values.isEmpty()) return BigDecimal.ZERO;
        int scale = values.stream()
                .filter(Objects::nonNull)
                .map(v -> v.stripTrailingZeros().scale())
                .max(Integer::compareTo)
                .orElse(0);
        scale = Math.min(Math.max(scale, 0), MAX_SCALE);

        BigInteger g = null;
        BigInteger tenPow = BigInteger.TEN.pow(scale);

        for (BigDecimal v : values) {
            if (v == null) continue;
            BigDecimal abs = v.abs();
            if (abs.signum() == 0) continue;

            BigInteger scaled;
            try {
                scaled = abs.multiply(new BigDecimal(tenPow))
                        .setScale(0, RoundingMode.UNNECESSARY)
                        .toBigIntegerExact();
            } catch (ArithmeticException ex) {
                scaled = abs.movePointRight(scale)
                        .setScale(0, RoundingMode.CEILING)
                        .toBigIntegerExact();
            }

            if (scaled.signum() == 0) continue;
            g = (g == null) ? scaled : g.gcd(scaled);
            if (BigInteger.ONE.equals(g)) break;
        }

        if (g == null || g.signum() == 0) return BigDecimal.ZERO;
        return new BigDecimal(g).movePointLeft(scale);
    }

    private static Optional<BigDecimal> minPositive(List<BigDecimal> values) {
        return values.stream().filter(Objects::nonNull).filter(v -> v.signum() > 0).min(Comparator.naturalOrder());
    }

    private static BigDecimal estimateTickFromOneSide(NavigableMap<BigDecimal, BigDecimal> asksAsc,
                                                      NavigableMap<BigDecimal, BigDecimal> bidsAsc) {
        BigDecimal ref = null;
        if (asksAsc != null && !asksAsc.isEmpty()) ref = asksAsc.firstKey();
        if (ref == null && bidsAsc != null && !bidsAsc.isEmpty()) ref = bidsAsc.firstKey();
        if (ref == null) return new BigDecimal("0.00000001");
        int scale = Math.min(Math.max(ref.stripTrailingZeros().scale(), 0), MAX_SCALE);
        return BigDecimal.ONE.scaleByPowerOfTen(-scale);
    }

    private static boolean isZeroOrNull(BigDecimal x) {
        return x == null || x.signum() == 0;
    }

    private static BigDecimal normalizeScale(BigDecimal x) {
        if (x == null) return new BigDecimal("0.00000001");
        int scale = Math.min(Math.max(x.stripTrailingZeros().scale(), 0), MAX_SCALE);
        return x.stripTrailingZeros().setScale(scale, RoundingMode.UNNECESSARY);
    }
}

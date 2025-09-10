package com.suhoi.mexcwebsocket.util;

import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class MarketMath {
    private MarketMath() {}

    /** floor к шагу step: ближайшее кратное step, ≤ value. */
    public static BigDecimal floorToStep(BigDecimal value, BigDecimal step) {
        if (value == null || step == null || step.signum() <= 0) return value;
        if (value.signum() <= 0) return BigDecimal.ZERO;
        BigDecimal multiples = value.divide(step, 0, RoundingMode.DOWN);
        return multiples.multiply(step);
    }

    /** Нормализация цены: floor к tick и защита от нуля. */
    public static BigDecimal normalizePrice(BigDecimal rawPrice, BigDecimal tick) {
        BigDecimal p = floorToStep(rawPrice, tick);
        if (p == null || p.signum() <= 0) {
            p = (tick != null && tick.signum() > 0) ? tick : new BigDecimal("0.00000001");
        }
        return p.stripTrailingZeros();
    }

    // -- Эффективный minNotional: если биржа не отдала, используем дефолт для USDT-пар
    public static BigDecimal resolveMinNotional(String symbol, BigDecimal exMinNotional) {
        if (exMinNotional != null && exMinNotional.compareTo(BigDecimal.ZERO) > 0) return exMinNotional;
        return (symbol != null && symbol.endsWith("USDT")) ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    // -- Минимально допустимое qty при заданной цене под minNotional (кратно stepSize)
    public static BigDecimal minQtyForNotional(BigDecimal price, BigDecimal stepSize, BigDecimal minNotional) {
        if (price == null || price.signum() <= 0) return BigDecimal.ZERO;
        if (minNotional == null || minNotional.signum() <= 0) return BigDecimal.ZERO;
        if (stepSize == null || stepSize.signum() <= 0) stepSize = BigDecimal.ONE;

        BigDecimal units = minNotional.divide(price, 0, RoundingMode.UP);
        BigDecimal k = units.divide(stepSize, 0, RoundingMode.UP);
        return k.multiply(stepSize).stripTrailingZeros();
    }

    /**
     * Корректирует и валидирует количество (до кратности stepSize).
     */
    public static BigDecimal normalizeQty(BigDecimal rawQty, SymbolFilters f) {
        BigDecimal q = floorToStep(rawQty, f.getStepSize());
        if (q == null) q = BigDecimal.ZERO;

        if (f.getMinQty().signum() > 0 && q.signum() > 0 && q.compareTo(f.getMinQty()) < 0) {
            BigDecimal neededMultiples = f.getMinQty().divide(f.getStepSize(), 0, RoundingMode.CEILING);
            q = neededMultiples.multiply(f.getStepSize());
            if (q.compareTo(rawQty) > 0) {
                q = floorToStep(rawQty, f.getStepSize());
            }
        }
        return q.stripTrailingZeros();
    }
}

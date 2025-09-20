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

    /** ceil к шагу step: ближайшее кратное step, ≥ value. Удобно для SELL у нижней кромки. */
    public static BigDecimal ceilToStep(BigDecimal value, BigDecimal step) {
        if (value == null || step == null || step.signum() <= 0) return value;
        if (value.signum() <= 0) return BigDecimal.ZERO;
        BigDecimal multiples = value.divide(step, 0, RoundingMode.UP);
        return multiples.multiply(step);
    }

    /** Нормализация цены: floor к tick и защита от нуля. (оставь как есть) */
    public static BigDecimal normalizePrice(BigDecimal rawPrice, BigDecimal tick) {
        BigDecimal p = floorToStep(rawPrice, tick);
        if (p == null || p.signum() <= 0) {
            p = (tick != null && tick.signum() > 0) ? tick : new BigDecimal("0.00000001");
        }
        return p.stripTrailingZeros();
    }

    /** Привести цену к «ceil» сетки тика: ближайший допустимый тик НЕ НИЖЕ raw. */
    public static BigDecimal alignPriceCeil(BigDecimal rawPrice, BigDecimal tickSize) {
        BigDecimal p = MarketMath.ceilToStep(rawPrice, tickSize);  // вверх к сетке
        // Защита от нуля/мусора: если raw<=0, вернём 1 тик
        if (p == null || p.signum() <= 0) {
            p = (tickSize != null && tickSize.signum() > 0) ? tickSize : new BigDecimal("0.00000001");
        }
        return p.stripTrailingZeros();
    }

    // -- Эффективный minNotional: если биржа не отдала, используем дефолт для USDT-пар
    public static BigDecimal resolveMinNotional(String symbol, BigDecimal exMinNotional) {
        if (exMinNotional != null && exMinNotional.compareTo(BigDecimal.ZERO) > 0) return exMinNotional;
        return (symbol != null && symbol.endsWith("USDT")) ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    // -- Минимально допустимое qty при заданной цене под minNotional (кратно stepSize)
    // MarketMath.java
    public static BigDecimal minQtyForNotional(BigDecimal price, BigDecimal stepSize, BigDecimal minNotional) {
        if (price == null || price.signum() <= 0) return BigDecimal.ZERO;
        if (minNotional == null || minNotional.signum() <= 0) return BigDecimal.ZERO;
        if (stepSize == null || stepSize.signum() <= 0) stepSize = BigDecimal.ONE;

        // Сколько «сырых» штук нужно на minNotional по текущей цене (с запасом вверх).
        // Дальше поднимем к сетке шага количества.
        BigDecimal rawQty = minNotional.divide(price, 18, RoundingMode.UP);

        // ceil к сетке stepSize: ближайшее кратное stepSize НЕ НИЖЕ rawQty
        BigDecimal steps = rawQty.divide(stepSize, 0, RoundingMode.UP);
        BigDecimal q = steps.multiply(stepSize);

        return q.stripTrailingZeros();
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

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
    public static BigDecimal clampInsideSpread(BigDecimal bid, BigDecimal ask, BigDecimal tick, BigDecimal pRaw) {
        if (bid == null || ask == null || tick == null || tick.signum() <= 0) return pRaw;
        if (ask.compareTo(bid) <= 0) {
            // нет спреда: вернём ceil(bid) — как нижняя кромка при нулевом спреде
            return alignPriceCeil(bid, tick);
        }
        BigDecimal nextAboveBid = floorToStep(bid, tick).add(tick);
        BigDecimal askMinusTick = ceilToStep(ask, tick).subtract(tick); // ← ключевая правка

        if (askMinusTick.compareTo(nextAboveBid) < 0) {
            // внутри спреда нет валидного тика: ставим на нижнюю кромку
            return nextAboveBid.stripTrailingZeros();
        }

        BigDecimal p = alignPriceCeil(pRaw, tick);
        if (p.compareTo(nextAboveBid) < 0) p = nextAboveBid;
        if (p.compareTo(askMinusTick) > 0) p = askMinusTick;
        return p.stripTrailingZeros();
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

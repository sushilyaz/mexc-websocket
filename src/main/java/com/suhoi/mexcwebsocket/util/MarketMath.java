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

    // =========================
    // Новые хелперы для клампа
    // =========================

    /**
     * Кламп к статическому диапазону [minPrice, maxPrice] с округлением к тикам.
     * Для SELL при поднятии к минимуму используем ceil (by tick), для BUY при опускании к максимуму — floor.
     */
    public static BigDecimal clampToStaticRange(BigDecimal rawPrice,
                                                BigDecimal minPrice,
                                                BigDecimal maxPrice,
                                                BigDecimal tick,
                                                boolean isSell) {
        if (rawPrice == null) return null;
        if (minPrice != null && maxPrice != null && minPrice.compareTo(maxPrice) > 0) {
            // перестраховка
            BigDecimal t = minPrice; minPrice = maxPrice; maxPrice = t;
        }

        BigDecimal p = rawPrice;
        if (minPrice != null && p.compareTo(minPrice) < 0) {
            p = isSell ? ceilToStep(minPrice, tick) : ceilToStep(minPrice, tick); // обеим сторонам лучше не опускаться ниже минимума
        }
        if (maxPrice != null && p.compareTo(maxPrice) > 0) {
            p = isSell ? floorToStep(maxPrice, tick) : floorToStep(maxPrice, tick);
        }
        // финальная подгонка к сетке тика в сторону заявки
        p = isSell ? ceilToStep(p, tick) : floorToStep(p, tick);
        return p.stripTrailingZeros();
    }

    /**
     * Кламп к процентному коридору вокруг референсной цены:
     * [ref * multiplierDown, ref * multiplierUp].
     * Для SELL — гарантируем p >= minAllowed (ceil к тику), для BUY — p <= maxAllowed (floor к тику).
     */
    public static BigDecimal clampToPercentBand(BigDecimal rawPrice,
                                                BigDecimal refPrice,
                                                BigDecimal multiplierDown,
                                                BigDecimal multiplierUp,
                                                BigDecimal tick,
                                                boolean isSell) {
        if (rawPrice == null || refPrice == null || refPrice.signum() <= 0) return rawPrice;
        if (multiplierDown == null || multiplierDown.signum() <= 0) multiplierDown = BigDecimal.ONE;
        if (multiplierUp == null || multiplierUp.signum() <= 0) multiplierUp = BigDecimal.ONE;

        BigDecimal minAllowed = refPrice.multiply(multiplierDown);
        BigDecimal maxAllowed = refPrice.multiply(multiplierUp);

        // округляем границы так, чтобы точно быть внутри коридора
        BigDecimal minAligned = ceilToStep(minAllowed, tick);
        BigDecimal maxAligned = floorToStep(maxAllowed, tick);

        BigDecimal p = rawPrice;

        if (isSell) {
            if (p.compareTo(minAligned) < 0) p = minAligned;
            if (p.compareTo(maxAligned) > 0) p = maxAligned; // на всякий
            p = ceilToStep(p, tick); // SELL — кверху
        } else {
            if (p.compareTo(maxAligned) > 0) p = maxAligned;
            if (p.compareTo(minAligned) < 0) p = minAligned; // на всякий
            p = floorToStep(p, tick); // BUY — вниз
        }
        return p.stripTrailingZeros();
    }
}

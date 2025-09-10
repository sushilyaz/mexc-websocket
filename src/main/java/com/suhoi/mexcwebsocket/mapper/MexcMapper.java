package com.suhoi.mexcwebsocket.mapper;

import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import com.suhoi.mexcwebsocket.mexc.rest.dto.response.SymbolInfoResponseDto;

import java.math.BigDecimal;

public class MexcMapper {
    private MexcMapper() {}

    /** Преобразует ответ MEXC в твой SymbolFilters c аккуратными fallback-ами. */
    public static SymbolFilters mapToFilters(SymbolInfoResponseDto dto, String symbol) {
        if (dto == null) {
            return defaultsFor(symbol);
        }

        // 1) quotePrecision
        Integer quotePrecision = firstNonNull(dto.getQuotePrecision(), dto.getQuoteAssetPrecision());
        if (quotePrecision == null) {
            quotePrecision = symbol != null && symbol.endsWith("USDT") ? 6 : 8; // как у тебя раньше
        }

        // 2) вытащим фильтры если есть
        SymbolInfoResponseDto.Filter priceFilter = getFilter(dto, "PRICE_FILTER");
        SymbolInfoResponseDto.Filter lotFilter   = getFilter(dto, "LOT_SIZE");
        SymbolInfoResponseDto.Filter notional    = firstNonNull(
                getFilter(dto, "MIN_NOTIONAL"),
                getFilter(dto, "NOTIONAL")
        );

        // 3) собираем значения
        BigDecimal tickSize = toBD(priceFilter != null ? priceFilter.getTickSize() : null);
        BigDecimal stepSize = toBD(lotFilter   != null ? lotFilter.getStepSize()   : null);
        BigDecimal minQty   = toBD(lotFilter   != null ? lotFilter.getMinQty()     : null);
        BigDecimal minNotional = toBD(notional != null ? notional.getMinNotional() : null);

        // 4) fallback-логика
        if (tickSize == null) {
            // «красивее», чем fixed 1e-8: если знаем точность котируемой, возьмём 10^-precision
            tickSize = quotePrecision != null
                    ? BigDecimal.ONE.movePointLeft(quotePrecision)
                    : new BigDecimal("0.00000001");
        }
        if (stepSize == null) {
            // у MEXC часто есть baseSizePrecision = реальный шаг количества (строкой)
            stepSize = toBD(dto.getBaseSizePrecision());
        }
        if (stepSize == null) stepSize = BigDecimal.ONE;

        // в старом коде default был 0 — сохраним поведение
        if (minQty == null) minQty = BigDecimal.ZERO;

        if (minNotional == null) minNotional = BigDecimal.ZERO;

        SymbolFilters out = new SymbolFilters(tickSize, stepSize, minQty, minNotional, quotePrecision);

        return out;
    }

    // ==== helpers ====

    private static SymbolInfoResponseDto.Filter getFilter(SymbolInfoResponseDto dto, String type) {
        if (dto.getFilters() == null) return null;
        for (var f : dto.getFilters()) {
            if (type.equals(f.getFilterType())) return f;
        }
        return null;
    }

    private static BigDecimal toBD(String s) {
        return (s == null || s.isBlank()) ? null : new BigDecimal(s);
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... vals) {
        for (T v : vals) if (v != null) return v;
        return null;
    }

    private static SymbolFilters defaultsFor(String symbol) {
        return new SymbolFilters(
                new BigDecimal("0.00000001"),
                BigDecimal.ONE,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                symbol != null && symbol.endsWith("USDT") ? 6 : 8
        );
    }


}

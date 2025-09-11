package com.suhoi.mexcwebsocket.mapper;

import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import com.suhoi.mexcwebsocket.mexc.rest.dto.response.SymbolInfoResponseDto;

import java.math.BigDecimal;

public class MexcMapper {
    private MexcMapper() {}

    /**
     * Маппинг ТОЛЬКО того, что реально даёт /exchangeInfo:
     * - quotePrecision
     * - minQty (из baseSizePrecision)
     * - minNotional (из quoteAmountPrecision, дефолт 1 для USDT-пар)
     * tickSize/stepSize здесь НЕ задаём — их нужно деривировать из стакана.
     */
    public static SymbolFilters mapToFilters(SymbolInfoResponseDto dto, String symbol) {
        if (dto == null) {
            // Вернём только то, что можно безопасно дефолтить: minNotional.
            return new SymbolFilters(
                    null,                     // tickSize -> из стакана
                    null,                     // stepSize -> из стакана
                    BigDecimal.ZERO,          // minQty неизвестен -> 0
                    defaultMinNotional(symbol),
                    defaultQuotePrecision(symbol) // чисто на случай, если вообще ничего не пришло
            );
        }

        // quotePrecision (форматная точность котируемой)
        Integer quotePrecision = firstNonNull(dto.getQuotePrecision(), dto.getQuoteAssetPrecision());
        if (quotePrecision == null) {
            quotePrecision = defaultQuotePrecision(symbol);
        }

        // minQty — из baseSizePrecision (док описывает как "min order quantity")
        BigDecimal minQty = toBD(dto.getBaseSizePrecision());
        if (minQty == null) minQty = BigDecimal.ZERO;

        // minNotional — из quoteAmountPrecision ("min order amount")
        BigDecimal minNotional = toBD(dto.getQuoteAmountPrecision());
        if (minNotional == null) {
            minNotional = defaultMinNotional(symbol);
        }

        // tickSize/stepSize из exchangeInfo часто отсутствуют (filters=[]), здесь их не трогаем
        return new SymbolFilters(
                null,            // tickSize -> из стакана
                null,            // stepSize -> из стакана
                minQty,
                minNotional,
                quotePrecision
        );
    }

    /** Слить (exchangeInfo ⊕ orderbook): exchangeInfo даёт minQty/minNotional/quotePrecision, стакан — tick/step. */
    public static SymbolFilters mergeWithBook(SymbolFilters ex, SymbolFilters book, String symbol) {
        if (ex == null) ex = new SymbolFilters(null, null, BigDecimal.ZERO, defaultMinNotional(symbol), defaultQuotePrecision(symbol));
        if (book == null) book = new SymbolFilters(null, null, BigDecimal.ZERO, defaultMinNotional(symbol), ex.getQuotePrecision());

        BigDecimal tick = nonZero(ex.getTickSize()) ? ex.getTickSize() : book.getTickSize();
        BigDecimal step = nonZero(ex.getStepSize()) ? ex.getStepSize() : book.getStepSize();

        BigDecimal minQty = nonZero(ex.getMinQty()) ? ex.getMinQty() : (nonZero(book.getMinQty()) ? book.getMinQty() : BigDecimal.ZERO);
        BigDecimal minNotional = nonZero(ex.getMinNotional()) ? ex.getMinNotional()
                : (nonZero(book.getMinNotional()) ? book.getMinNotional() : defaultMinNotional(symbol));

        Integer qp = (ex.getQuotePrecision() != null) ? ex.getQuotePrecision()
                : (book.getQuotePrecision() != null) ? book.getQuotePrecision()
                : (tick != null ? Math.max(0, tick.stripTrailingZeros().scale()) : defaultQuotePrecision(symbol));

        return new SymbolFilters(tick, step, minQty, minNotional, qp);
    }

    // ==== helpers ====

    private static BigDecimal toBD(String s) {
        return (s == null || s.isBlank() || "0".equals(s)) ? null : new BigDecimal(s);
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... vals) {
        for (T v : vals) if (v != null) return v;
        return null;
    }

    private static boolean nonZero(BigDecimal x) {
        return x != null && x.signum() > 0;
    }

    private static BigDecimal defaultMinNotional(String symbol) {
        return (symbol != null && symbol.endsWith("USDT")) ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    private static int defaultQuotePrecision(String symbol) {
        // На практике USDT-пары обычно 6–8; пусть будет 6 для USDT, иначе 8 — чисто запасной вариант
        return (symbol != null && symbol.endsWith("USDT")) ? 6 : 8;
    }
}

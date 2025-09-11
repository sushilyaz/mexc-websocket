package com.suhoi.mexcwebsocket.mexc.rest.dto.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SymbolInfoResponseDto {
    private String symbol;
    private String status;
    private String baseAsset;
    private Integer baseAssetPrecision;
    private String quoteAsset;
    private Integer quotePrecision;
    private Integer quoteAssetPrecision;
    private Integer baseCommissionPrecision;
    private Integer quoteCommissionPrecision;

    private List<String> orderTypes;
    private Boolean isSpotTradingAllowed;
    private Boolean isMarginTradingAllowed;

    private String quoteAmountPrecision;          // напр. "1" — точность суммы для MARKET по quote
    private String baseSizePrecision;             // напр. "0.1" — шаг по количеству base
    private String quoteAmountPrecisionMarket;    // напр. "1"
    private String maxQuoteAmount;
    private String maxQuoteAmountMarket;

    private List<String> permissions;
    private List<Filter> filters;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Filter {
        private String filterType;    // PRICE_FILTER / LOT_SIZE / MIN_NOTIONAL
        private String tickSize;      // для PRICE_FILTER
        private String stepSize;      // для LOT_SIZE
        private String minQty;        // для LOT_SIZE
        private String minNotional;   // для MIN_NOTIONAL
    }
}

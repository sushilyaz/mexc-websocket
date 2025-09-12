package com.suhoi.mexcwebsocket.mexc.rest.dto.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountInfoResponseDto {

    private BigDecimal makerCommission;
    private BigDecimal takerCommission;
    private BigDecimal buyerCommission;
    private BigDecimal sellerCommission;

    private Boolean canTrade;
    private Boolean canWithdraw;
    private Boolean canDeposit;

    private Long updateTime;
    private String accountType;

    private List<Balance> balances;
    private List<String> permissions;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Balance {
        private String asset;
        private BigDecimal free;
        private BigDecimal locked;
        private BigDecimal available;
    }
}

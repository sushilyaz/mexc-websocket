package com.suhoi.mexcwebsocket.mexc.rest.dto.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExchangeInfoResponseDto {
    private List<SymbolInfoResponseDto> symbols;
}

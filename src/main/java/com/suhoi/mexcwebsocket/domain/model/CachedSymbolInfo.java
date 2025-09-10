package com.suhoi.mexcwebsocket.domain.model;

import com.suhoi.mexcwebsocket.mexc.rest.dto.response.SymbolInfoResponseDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class CachedSymbolInfo {
    private SymbolFilters filters;
    private long loadedAt;
}

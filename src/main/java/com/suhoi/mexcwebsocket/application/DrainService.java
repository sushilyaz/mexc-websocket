// application/DrainService.java
package com.suhoi.mexcwebsocket.application;

import com.suhoi.mexcwebsocket.db.MemoryDb;
import com.suhoi.mexcwebsocket.domain.model.Creds;
import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import com.suhoi.mexcwebsocket.mexc.rest.MexcRestFacade;
import com.suhoi.mexcwebsocket.mexc.ws.market.OrderBookService;
import com.suhoi.mexcwebsocket.mexc.ws.user.UserStreamRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

import static com.suhoi.mexcwebsocket.util.FormatHelpers.fmt;

@Service
@Slf4j
@RequiredArgsConstructor
public class DrainService {
    private final UserStreamRegistry userStreams;
    private final OrderBookService orderBooks;
    private final MexcRestFacade mexcRestFacade;

    public void startDrain(String symbol, BigDecimal usdtAmount, Long chatId) {
        Creds credsA = MemoryDb.getAccountA(chatId);
        Creds credsB = MemoryDb.getAccountB(chatId);

        log.info("🚀 START_DRAIN chatId={} symbol='{}' amount={} USDT", chatId, symbol, fmt(usdtAmount));

        if (credsA != null) {
            userStreams.startOrUpdate(chatId, UserStreamRegistry.Slot.A, credsA);
        } else {
            log.warn("⚠️ No creds for AccountA (chatId={})", chatId);
        }
        if (credsB != null) {
            userStreams.startOrUpdate(chatId, UserStreamRegistry.Slot.B, credsB);
        } else {
            log.warn("⚠️ No creds for AccountB (chatId={})", chatId);
        }

        // ⬇️ включаем L2 для указанного символа (если не пустой)
        if (symbol != null && !symbol.isBlank()) {
            orderBooks.startTracking(symbol);
            log.info("🧱 L2 initialized for {}", symbol.toUpperCase());
        } else {
            log.warn("⚠️ Symbol is empty — L2 not started");
        }

        // Получаем характеристики монеты для дальнейшего рассчета(tickSize, stepSize, minNotional, minQty, quotePrecision)
        SymbolFilters symbolFilters = mexcRestFacade.getSymbolFilters(symbol);

        // покупаем по рынку путем выставления лимитки над спредом
        String orderId = mexcRestFacade.limitBuyAboveSpreadA(symbol, usdtAmount, chatId);

    }
}

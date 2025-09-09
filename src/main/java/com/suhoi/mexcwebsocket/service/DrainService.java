package com.suhoi.mexcwebsocket.service;

import com.suhoi.mexcwebsocket.db.MemoryDb;
import com.suhoi.mexcwebsocket.model.Creds;
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

        // Здесь — остальная логика дренажа, если нужна
    }
}

// src/main/java/com/suhoi/mexcwebsocket/adapter/telegram/TelegramBotHandler.java
package com.suhoi.mexcwebsocket.adapter.telegram;

import com.suhoi.mexcwebsocket.config.AppProperties;
import com.suhoi.mexcwebsocket.db.MemoryDb;
import com.suhoi.mexcwebsocket.domain.model.Creds;
import com.suhoi.mexcwebsocket.application.DrainService;
import com.suhoi.mexcwebsocket.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.objects.Update;

import java.math.BigDecimal;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelegramBotHandler extends TelegramLongPollingBot {

    private final AppProperties appProperties;
    private final TelegramService tg;
    private final DrainService drainService;

    @Override
    public String getBotUsername() {
        return appProperties.getTelegram().getBotUsername();
    }

    @Override
    public String getBotToken() {
        return appProperties.getTelegram().getBotToken();
    }

    @Override
    public void onUpdateReceived(Update update) {
        if (update.getMessage() == null || update.getMessage().getText() == null) return;

        final long chatId = update.getMessage().getChatId();
        final String text = update.getMessage().getText().trim();

        try {
            if (text.startsWith("/start")) {
                tg.reply(chatId, """
                        Привет! Я бот для перелива через спред на MEXC.

                        Ключи:
                        /setA <apiKey> <secretKey> — задать ключи Аккаунта A (С КОТОРОГО переливаем)
                        /setB <apiKey> <secretKey> — задать ключи Аккаунта B (НА КОТОРЫЙ переливаем)

                        Параметры алгоритма:
                        /ticks <N> — количество тиков над/под спредом для агрессивной лимитки (сейчас: %d)
                        /set spread_guard <VAL> — расстояние от границ спреда, 0 < VAL < 0.5 (сейчас: %s)

                        Режимы перелива:
                        1) Простой:   /drain <SYMBOL> <USDT>
                           пример: /drain ANTUSDT 5

                        Сервис:
                        /status — показать текущее состояние перелива
                        /stop — ручная пауза
                        /continue <SYMBOL> [cycles] — продолжить из фактических балансов
                        """.formatted(
                        Constants.TICK_ABOVE,
                        Constants.SPREAD_GUARD.stripTrailingZeros().toPlainString()
                ));
                return;
            }

            if (text.startsWith("/setA")) {
                String[] p = text.split("\\s+");
                if (p.length != 3) {
                    tg.reply(chatId, "Формат: /setA <apiKey> <secretKey>");
                    return;
                }
                MemoryDb.setAccountA(chatId, new Creds(p[1], p[2]));
                tg.reply(chatId, "✅ Ключи Аккаунта A сохранены (in-memory).");
                return;
            }

            if (text.startsWith("/setB")) {
                String[] p = text.split("\\s+");
                if (p.length != 3) {
                    tg.reply(chatId, "Формат: /setB <apiKey> <secretKey>");
                    return;
                }
                MemoryDb.setAccountB(chatId, new Creds(p[1], p[2]));
                tg.reply(chatId, "✅ Ключи Аккаунта B сохранены (in-memory).");
                return;
            }

            // ===== /ticks <N> — задаём TICK_ABOVE =====
            if (text.startsWith("/ticks")) {
                String[] p = text.split("\\s+");
                if (p.length != 2) {
                    tg.reply(chatId, "Формат: /ticks <N>\nНапример: /ticks 3");
                    return;
                }
                int n;
                try {
                    n = Integer.parseInt(p[1]);
                } catch (Exception e) {
                    tg.reply(chatId, "N должно быть целым числом.");
                    return;
                }
                if (n <= 0) {
                    tg.reply(chatId, "N должно быть > 0.");
                    return;
                }
                Constants.TICK_ABOVE = n;
                tg.reply(chatId, "✅ Установлено: TICK_ABOVE = " + n);
                return;
            }

            // ===== /set spread_guard <VAL> — задаём SPREAD_GUARD =====
            if (text.startsWith("/set ")) {
                String[] p = text.split("\\s+");
                if (p.length == 3 && "spread_guard".equalsIgnoreCase(p[1])) {
                    BigDecimal val = parseDecimalSafe(p[2]);
                    if (val == null) {
                        tg.reply(chatId, "VAL должно быть числом. Пример: /set spread_guard 0.20");
                        return;
                    }
                    if (val.compareTo(BigDecimal.ZERO) <= 0 || val.compareTo(new BigDecimal("0.5")) >= 0) {
                        tg.reply(chatId, "Некорректно: требование 0 < VAL < 0.5");
                        return;
                    }
                    Constants.SPREAD_GUARD = val.stripTrailingZeros();
                    tg.reply(chatId, "✅ Установлено: SPREAD_GUARD = " + Constants.SPREAD_GUARD.toPlainString());
                    return;
                }
            }

            // ===== /status =====
            if (text.startsWith("/status")) {
                tg.reply(chatId, drainService.status(chatId));
                return;
            }

            if (text.startsWith("/stop")) {
                tg.reply(chatId, "⏸ Поставил на паузу (MANUAL).");
                return;
            }

            if (text.startsWith("/continue")) {
                String[] p = text.split("\\s+");
                if (p.length < 2) {
                    tg.reply(chatId, "Формат: /continue <SYMBOL> [cycles]\nпример: /continue ANTUSDT 20");
                    return;
                }
                String symbol = p[1].toUpperCase();
                int cycles = (p.length >= 3) ? Integer.parseInt(p[2]) : 20;
                tg.reply(chatId, "▶️ Продолжаю из фактических балансов по %s".formatted(symbol));
                // drainService.continueFromBalances(symbol, chatId, cycles);
                return;
            }

            if (text.startsWith("/drain")) {
                String[] p = text.split("\\s+");
                var a = MemoryDb.getAccountA(chatId);
                var b = MemoryDb.getAccountB(chatId);
                if (a == null || b == null) {
                    tg.reply(chatId, "Сначала задайте ключи: /setA и /setB");
                    return;
                }
                if (p.length == 3) {
                    final String symbol = p[1].toUpperCase();
                    final BigDecimal usdt = parseDecimalSafe(p[2]);
                    if (usdt == null) {
                        tg.reply(chatId, "Сумма USDT должна быть числом. Пример: /drain ANTUSDT 5");
                        return;
                    }
                    tg.reply(chatId, "▶️ Запускаю перелив: %s на %s USDT".formatted(symbol, usdt.stripTrailingZeros()));
                    drainService.startDrain(symbol, usdt, chatId);
                    return;
                }
                tg.reply(chatId, "Неверный формат. Пример: /drain ANTUSDT 5");
                return;
            }

            tg.reply(chatId, "Неизвестная команда. Наберите /start");

        } catch (Exception ex) {
            log.error("Ошибка обработки апдейта", ex);
            tg.reply(chatId, "❌ Ошибка: " + ex.getMessage());
        }
    }

    // --- Utils ---

    private static BigDecimal parseDecimalSafe(String s) {
        if (s == null) return null;
        try {
            return new BigDecimal(s.trim().replace(',', '.'));
        } catch (Exception e) {
            return null;
        }
    }
}

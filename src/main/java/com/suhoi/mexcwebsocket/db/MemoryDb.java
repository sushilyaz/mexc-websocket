package com.suhoi.mexcwebsocket.db;

import com.suhoi.mexcwebsocket.domain.model.Creds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryDb {
    private static final Map<Long, Creds> accountA = new ConcurrentHashMap<>();
    private static final Map<Long, Creds> accountB = new ConcurrentHashMap<>();

    public static Creds getAccountA(Long chatId) { return accountA.get(chatId); }
    public static Creds getAccountB(Long chatId) { return accountB.get(chatId); }
    public static void setAccountA(Long chatId, Creds c) { accountA.put(chatId, c); }
    public static void setAccountB(Long chatId, Creds c) { accountB.put(chatId, c); }
}

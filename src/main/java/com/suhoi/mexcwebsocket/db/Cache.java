package com.suhoi.mexcwebsocket.db;

import com.suhoi.mexcwebsocket.domain.model.CachedSymbolInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Cache {
    public static final Map<String, CachedSymbolInfo> exchangeInfoCache = new ConcurrentHashMap<>();
}

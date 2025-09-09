package com.suhoi.mexcwebsocket.mexc.ws.user;

import com.suhoi.mexcwebsocket.mexc.ws.core.MexcWsClient;
import com.suhoi.mexcwebsocket.config.MexcUserProps;
import com.suhoi.mexcwebsocket.config.MexcWsProps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.*;

@Slf4j
public class MexcUserStreamService {

    private final MexcWsProps wsBaseProps;
    private final MexcUserProps userProps;
    private final ListenKeyClient listenKeyClient;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "mexc-user-lk");
        t.setDaemon(true); return t;
    });

    private volatile String listenKey;
    @Getter
    private volatile MexcWsClient client; // отдельный клиент под user WS

    public MexcUserStreamService(MexcWsProps wsBaseProps, MexcUserProps userProps, ListenKeyClient lk) {
        this.wsBaseProps = wsBaseProps;
        this.userProps = userProps;
        this.listenKeyClient = lk;
    }

    public void start() {
        if (!userProps.isEnabled()) {
            log.info("[USER] disabled");
            return;
        }
        Objects.requireNonNull(userProps.getApiKey(),  "mexc.user.apiKey required");
        Objects.requireNonNull(userProps.getSecret(),  "mexc.user.secret required");

        // 1) создать listenKey
        this.listenKey = listenKeyClient.createListenKey(userProps.getApiKey(), userProps.getSecret());

        // 2) собрать endpoint вида wss://.../ws?listenKey=...
        String endpointWithKey = wsBaseProps.getEndpoint() + "?listenKey=" + listenKey;

        // 3) поднять отдельный MexcWsClient c теми же настройками, но другим endpoint
        MexcWsProps copy = new MexcWsProps();
        copy.setEnabled(true);
        copy.setEndpoint(endpointWithKey);
        copy.setPingPeriodSec(wsBaseProps.getPingPeriodSec());
        copy.setRotateAfterHours(wsBaseProps.getRotateAfterHours()); // < 24h
        copy.setReconnectBackoffBaseSec(wsBaseProps.getReconnectBackoffBaseSec());
        copy.setReconnectBackoffMaxSec(wsBaseProps.getReconnectBackoffMaxSec());
        copy.setMaxSubscriptionsPerConn(wsBaseProps.getMaxSubscriptionsPerConn());

        this.client = new MexcWsClient(copy);
        this.client.connect();

        // 4) keepalive listenKey
        int period = Math.max(5, userProps.getKeepAliveMinutes());
        scheduler.scheduleAtFixedRate(this::safeKeepAlive, period, period, TimeUnit.MINUTES);

        log.info("[USER] started, endpoint={}", endpointWithKey);
    }

    public void stop() {
        scheduler.shutdownNow();
        try {
            if (client != null) client.close();
        } catch (Exception ignore) {}
        if (listenKey != null) {
            try {
                listenKeyClient.close(userProps.getApiKey(), userProps.getSecret(), listenKey);
            } catch (Exception e) {
                log.debug("[USER] close listenKey err: {}", e.getMessage());
            }
        }
        log.info("[USER] stopped");
    }

    private void safeKeepAlive() {
        try {
            listenKeyClient.keepAlive(userProps.getApiKey(), userProps.getSecret(), listenKey);
            log.debug("[USER] keepalive OK");
        } catch (Exception e) {
            // если 4xx по key (истёк/удалён) — создадим новый и переподнимем WS
            log.warn("[USER] keepalive failed: {}. Recreate listenKey...", e.toString());
            try {
                String newKey = listenKeyClient.createListenKey(userProps.getApiKey(), userProps.getSecret());
                String newEndpoint = wsBaseProps.getEndpoint() + "?listenKey=" + newKey;
                this.listenKey = newKey;
                // мягкая ротация: закроем текущий WS — он переподнимется новым экземпляром
                if (client != null) client.close();
                MexcWsProps copy = new MexcWsProps();
                copy.setEnabled(true);
                copy.setEndpoint(newEndpoint);
                copy.setPingPeriodSec(wsBaseProps.getPingPeriodSec());
                copy.setRotateAfterHours(wsBaseProps.getRotateAfterHours());
                copy.setReconnectBackoffBaseSec(wsBaseProps.getReconnectBackoffBaseSec());
                copy.setReconnectBackoffMaxSec(wsBaseProps.getReconnectBackoffMaxSec());
                copy.setMaxSubscriptionsPerConn(wsBaseProps.getMaxSubscriptionsPerConn());
                this.client = new MexcWsClient(copy);
                this.client.connect();
                log.info("[USER] switched to new listenKey, endpoint={}", newEndpoint);
            } catch (Exception ex) {
                log.error("[USER] failed to recreate listenKey: {}", ex.toString());
            }
        }
    }
}


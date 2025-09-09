package com.suhoi.mexcwebsocket.service;

import com.suhoi.mexcwebsocket.client.MexcWsClient;
import com.suhoi.mexcwebsocket.config.MexcWsProps;
import com.suhoi.mexcwebsocket.model.Creds;
import com.suhoi.mexcwebsocket.util.ListenKeyClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.*;

@Slf4j
public class UserStreamSession {

    private final MexcWsProps baseWsProps;
    private final ListenKeyClient listenKeyClient;
    private final Creds creds;
    private final MexcWsClient.Listener listener;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "mexc-user-session");
        t.setDaemon(true); return t;
    });

    private volatile String listenKey;
    private volatile MexcWsClient client;

    public UserStreamSession(MexcWsProps baseWsProps, ListenKeyClient listenKeyClient,
                             Creds creds, MexcWsClient.Listener listener) {
        this.baseWsProps = Objects.requireNonNull(baseWsProps);
        this.listenKeyClient = Objects.requireNonNull(listenKeyClient);
        this.creds = Objects.requireNonNull(creds);
        this.listener = Objects.requireNonNull(listener);
    }

    public boolean sameCreds(Creds other) {
        return other != null
                && Objects.equals(creds.getApiKey(), other.getApiKey())
                && Objects.equals(creds.getSecret(), other.getSecret());
    }

    /** Создаём listenKey, открываем WS и подписываемся на приватные каналы. */
    public void startAndSubscribe() {
        // 1) create listenKey
        this.listenKey = listenKeyClient.createListenKey(creds.getApiKey(), creds.getSecret());

        // 2) собрать endpoint с параметром listenKey
        String endpoint = baseWsProps.getEndpoint() + "?listenKey=" + listenKey;

        // 3) сделать копию WS-настроек под этот endpoint
        MexcWsProps copy = new MexcWsProps();
        copy.setEnabled(true);
        copy.setEndpoint(endpoint);
        copy.setPingPeriodSec(baseWsProps.getPingPeriodSec());
        copy.setRotateAfterHours(baseWsProps.getRotateAfterHours());
        copy.setReconnectBackoffBaseSec(baseWsProps.getReconnectBackoffBaseSec());
        copy.setReconnectBackoffMaxSec(baseWsProps.getReconnectBackoffMaxSec());
        copy.setMaxSubscriptionsPerConn(baseWsProps.getMaxSubscriptionsPerConn());

        // 4) поднять MexcWsClient
        this.client = new MexcWsClient(copy);
        this.client.addListener(listener);
        this.client.connect();

        // 5) приватные подписки (очередь SUB сработает до onOpen)
        this.client.subscribe(
                "spot@private.account.v3.api.pb",
                "spot@private.deals.v3.api.pb",
                "spot@private.orders.v3.api.pb"
        );

        // 6) keepalive каждые 30 минут (можешь вынести в props при желании)
        scheduler.scheduleAtFixedRate(this::safeKeepAlive, 30, 30, TimeUnit.MINUTES);
    }

    public void stop() {
        scheduler.shutdownNow();
        try {
            if (client != null) client.close();
        } catch (Exception ignore) {}
        try {
            if (listenKey != null) listenKeyClient.close(creds.getApiKey(), creds.getSecret(), listenKey);
        } catch (Exception e) {
            log.debug("[USER] close key err: {}", e.toString());
        }
    }

    private void safeKeepAlive() {
        try {
            listenKeyClient.keepAlive(creds.getApiKey(), creds.getSecret(), listenKey);
            log.debug("[USER] keepalive OK");
        } catch (Exception e) {
            log.warn("[USER] keepalive failed: {}. Recreate listenKey...", e.toString());
            try {
                String newKey = listenKeyClient.createListenKey(creds.getApiKey(), creds.getSecret());
                this.listenKey = newKey;
                // мягкая ротация: переподнимем WS на новый endpoint
                if (client != null) client.close();

                MexcWsProps copy = new MexcWsProps();
                copy.setEnabled(true);
                copy.setEndpoint(baseWsProps.getEndpoint() + "?listenKey=" + newKey);
                copy.setPingPeriodSec(baseWsProps.getPingPeriodSec());
                copy.setRotateAfterHours(baseWsProps.getRotateAfterHours());
                copy.setReconnectBackoffBaseSec(baseWsProps.getReconnectBackoffBaseSec());
                copy.setReconnectBackoffMaxSec(baseWsProps.getReconnectBackoffMaxSec());
                copy.setMaxSubscriptionsPerConn(baseWsProps.getMaxSubscriptionsPerConn());

                this.client = new MexcWsClient(copy);
                this.client.addListener(listener);
                this.client.connect();
                this.client.subscribe(
                        "spot@private.account.v3.api.pb",
                        "spot@private.deals.v3.api.pb",
                        "spot@private.orders.v3.api.pb"
                );
                log.info("[USER] switched to new listenKey");
            } catch (Exception ex) {
                log.error("[USER] failed to recreate listenKey: {}", ex.toString());
            }
        }
    }
}


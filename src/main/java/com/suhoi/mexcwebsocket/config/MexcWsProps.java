package com.suhoi.mexcwebsocket.config;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "mexc.ws")
public class MexcWsProps {
    private boolean enabled = true;
    private String endpoint = "wss://wbs-api.mexc.com/ws";
    private int pingPeriodSec = 25;
    private int rotateAfterHours = 23;
    private int reconnectBackoffBaseSec = 2;
    private int reconnectBackoffMaxSec = 60;
    private int maxSubscriptionsPerConn = 30;
}

package com.suhoi.mexcwebsocket.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "mexc.user")
public class MexcUserProps {
    private boolean enabled = false;
    private String apiKey;
    private String secret;
    private int keepAliveMinutes = 30;
}


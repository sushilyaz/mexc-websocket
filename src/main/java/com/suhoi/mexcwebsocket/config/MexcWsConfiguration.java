package com.suhoi.mexcwebsocket.config;

import com.suhoi.mexcwebsocket.mexc.ws.core.MexcWsClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MexcWsProps.class)
public class MexcWsConfiguration {

    @Bean(initMethod = "connect", destroyMethod = "close")
    @ConditionalOnProperty(prefix = "mexc.ws", name = "enabled", havingValue = "true", matchIfMissing = true)
    public MexcWsClient mexcWsClient(MexcWsProps props) {
        return new MexcWsClient(props);
    }
}

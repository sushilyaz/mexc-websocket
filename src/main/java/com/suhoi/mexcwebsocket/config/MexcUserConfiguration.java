package com.suhoi.mexcwebsocket.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcwebsocket.service.MexcUserStreamService;
import com.suhoi.mexcwebsocket.util.ListenKeyClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Configuration
@EnableConfigurationProperties({MexcUserProps.class})
public class MexcUserConfiguration {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public ListenKeyClient listenKeyClient(RestTemplate rt, ObjectMapper om) {
        return new ListenKeyClient(rt, om);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(prefix = "mexc.user", name = "enabled", havingValue = "true")
    public MexcUserStreamService mexcUserStreamService(
            MexcWsProps wsProps,
            MexcUserProps userProps,
            ListenKeyClient listenKeyClient
    ) {
        return new MexcUserStreamService(wsProps, userProps, listenKeyClient);
    }
}


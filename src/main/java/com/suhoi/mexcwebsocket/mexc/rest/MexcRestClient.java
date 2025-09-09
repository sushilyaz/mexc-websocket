// mexc/rest/MexcRestClient.java
package com.suhoi.mexcwebsocket.mexc.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

@Service
public class MexcRestClient {
    private final RestTemplate rest;

    public MexcRestClient(RestTemplateBuilder builder) {
        this.rest = builder
                .rootUri("https://api.mexc.com")
                .build();
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DepthDto {
        private List<List<String>> bids;
        private List<List<String>> asks;
        @JsonProperty("lastUpdateId")
        private Long lastUpdateId;
    }

    public DepthDto getDepthSnapshot(String symbol, int limit) {
        String uri = UriComponentsBuilder.fromPath("/api/v3/depth")
                .queryParam("symbol", symbol.toUpperCase())
                .queryParam("limit", limit)
                .build()
                .toUriString();
        return rest.getForObject(uri, DepthDto.class);
    }

    public static BigDecimal bd(String s) { return new BigDecimal(s); }
}

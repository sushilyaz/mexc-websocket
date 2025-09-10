// mexc/rest/MexcRestClient.java
package com.suhoi.mexcwebsocket.mexc.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcwebsocket.mexc.rest.dto.response.DepthResponseDto;
import com.suhoi.mexcwebsocket.mexc.rest.dto.response.SymbolInfoResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MexcRestClient {
    private static final String API_BASE = "https://api.mexc.com";
    private static final String API_PREFIX = "/api/v3";
    private static final String DEPTH_OB_ENDPOINT = "/depth";
    private static final String ORDER_ENDPOINT = "/order";
    private static final String TIME_ENDPOINT = "/time";
    private static final String EXCHANGE_INFO_ENDPOINT = "/exchangeInfo";
    private final RestTemplate rest;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MexcRestClient(RestTemplateBuilder builder) {
        this.rest = builder
                .rootUri(API_BASE)
                .build();
    }

    public DepthResponseDto getDepthSnapshot(String symbol, int limit) {
        String uri = UriComponentsBuilder.fromPath(API_PREFIX + DEPTH_OB_ENDPOINT)
                .queryParam("symbol", symbol.toUpperCase())
                .queryParam("limit", limit)
                .build()
                .toUriString();
        return rest.getForObject(uri, DepthResponseDto.class);
    }

    public SymbolInfoResponseDto getSymbolInformation(String symbol) {
        String uri = UriComponentsBuilder.fromPath(API_PREFIX + EXCHANGE_INFO_ENDPOINT)
                .queryParam("symbol", symbol.toUpperCase())
                .build()
                .toUriString();
        return rest.getForObject(uri, SymbolInfoResponseDto.class);
    }

    public String newOrderBuyLimitIoc(String symbol, String qty, String price, String clientId,
                                      String apiKey, String secretKey) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("symbol", symbol);
        p.put("side", "BUY");
        p.put("type", "LIMIT");
        p.put("timeInForce", "IOC");
        p.put("quantity", qty);
        p.put("price", price);
        p.put("newClientOrderId", clientId);

        JsonNode resp;
        try {
            resp = signedRequest("POST", ORDER_ENDPOINT, p, apiKey, secretKey);
        } catch (RuntimeException ex1) {
            String msg = (ex1.getMessage() == null) ? "" : ex1.getMessage().toLowerCase();
            // фоллбэк на documented тип
            if (msg.contains("timeinforce") || msg.contains("illegal") || msg.contains("parameter")) {
                log.warn("[NEW_ORDER] {}: timeInForce=IOC rejected → try type=IMMEDIATE_OR_CANCEL", symbol);
                p.remove("timeInForce");
                p.put("type", "IMMEDIATE_OR_CANCEL");
                try {
                    resp = signedRequest("POST", ORDER_ENDPOINT, p, apiKey, secretKey);
                } catch (RuntimeException ex2) {
                    log.warn("[NEW_ORDER] {}: IMMEDIATE_OR_CANCEL rejected → try LIMIT GTC", symbol);
                    p.put("type", "LIMIT");
                    p.put("timeInForce", "GTC");
                    resp = signedRequest("POST", ORDER_ENDPOINT, p, apiKey, secretKey);
                }
            } else {
                throw ex1;
            }
        }

        String orderId = resp.path("orderId").asText(null);
        if (orderId == null) {
            log.error("[NEW_ORDER] {} clientId={} → no orderId in response: {}", symbol, clientId, resp);
            return null;
        }
        log.info("[NEW_ORDER] {} clientId={} orderId={}", symbol, clientId, orderId);
        return orderId;
    }



    public long getServerTime() {
        try {
            String url = API_BASE + API_PREFIX + TIME_ENDPOINT;
            String body = rest.getForObject(url, String.class);
            JsonNode j = objectMapper.readTree(body);
            return j.get("serverTime").asLong();
        } catch (Exception e) {
            log.warn("Не удалось получить serverTime: {}", e.getMessage());
            return System.currentTimeMillis();
        }
    }
    private String hmacSha256Hex(String data, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] h = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(h.length * 2);
        for (byte b : h) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private String buildCanonical(Map<String, String> params) {
        return params.entrySet().stream()
                .map(e -> e.getKey() + "=" + (e.getValue() == null ? "" : e.getValue()))
                .collect(Collectors.joining("&"));
    }
    /**
     * Универсальный подписанный запрос. Для GET/DELETE/POST.
     */
    private JsonNode signedRequest(String method, String path, Map<String, String> params, String apiKey, String secret) {
        try {
            if (params == null) params = new LinkedHashMap<>();
            params.put("timestamp", String.valueOf(getServerTime()));
            params.put("recvWindow", "5000");

            String canonical = buildCanonical(params);
            String signature = hmacSha256Hex(canonical, secret);
            params.put("signature", signature);

            String finalQuery = buildCanonical(params);
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-MEXC-APIKEY", apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Void> entity = new HttpEntity<>(headers);

            HttpMethod httpMethod = switch (method.toUpperCase()) {
                case "GET" -> HttpMethod.GET;
                case "DELETE" -> HttpMethod.DELETE;
                default -> HttpMethod.POST;
            };
            log.info("{} https://api.mexc.com{}?{}", httpMethod.name(), path, finalQuery);
            ResponseEntity<String> resp = rest.exchange(API_BASE + API_PREFIX + path + "?" + finalQuery, httpMethod, entity, String.class);
            return objectMapper.readTree(resp.getBody());

        } catch (org.springframework.web.client.HttpClientErrorException e) {
            String body = e.getResponseBodyAsString();
            log.error("HTTP {} {} -> status={}, body={}", method, path, e.getStatusCode(), body);
            throw new RuntimeException("Signed request error: " + e.getStatusCode() + " - " + body, e);
        } catch (Exception e) {
            throw new RuntimeException("Signed request error: " + e.getMessage(), e);
        }
    }

    public static BigDecimal bd(String s) {
        return new BigDecimal(s);
    }
}

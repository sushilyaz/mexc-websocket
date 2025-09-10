package com.suhoi.mexcwebsocket.mexc.rest.dto.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DepthResponseDto {
    private List<List<String>> bids;
    private List<List<String>> asks;
    @JsonProperty("lastUpdateId")
    private Long lastUpdateId;
}

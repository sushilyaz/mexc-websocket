package com.suhoi.mexcwebsocket.domain.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Creds {
    private String apiKey;
    private String secret;
}

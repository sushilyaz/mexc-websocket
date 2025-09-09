package com.suhoi.mexcwebsocket.service;

import com.mxc.push.common.protobuf.PrivateAccountV3Api;
import com.mxc.push.common.protobuf.PrivateDealsV3Api;
import com.mxc.push.common.protobuf.PrivateOrdersV3Api;
import com.suhoi.mexcwebsocket.client.MexcWsClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnBean(MexcUserStreamService.class)
@RequiredArgsConstructor
public class DemoUserSubscriberService implements MexcWsClient.Listener {

    private final MexcUserStreamService userService;

    @PostConstruct
    void init() {
        MexcWsClient c = userService.getClient();
        c.addListener(this);

        // Можно подписаться сразу — у MexcWsClient очередь SUB до onOpen уже есть.
        c.subscribe(
                "spot@private.account.v3.api.pb",
                "spot@private.deals.v3.api.pb",
                "spot@private.orders.v3.api.pb"
        );
        log.info("▶️ UserSubscriber subscribed to account/deals/orders");
    }

    @PreDestroy
    void shutdown() {
        MexcWsClient c = userService.getClient();
        if (c != null) c.removeListener(this);
    }

    @Override public void onOpen() { log.info("✅ USER WS OPEN"); }
    @Override public void onAck(String json) { log.info("✅ USER ACK {}", json); }
    @Override public void onPong() { log.info("🏓 USER PONG"); }

    @Override
    public void onPrivateAccount(PrivateAccountV3Api acc, long ts) {
        log.info("👤 Balance {} bal={} (Δ={} ), frozen={} (Δ={} ), type={}, time={}",
                acc.getVcoinName(), acc.getBalanceAmount(), acc.getBalanceAmountChange(),
                acc.getFrozenAmount(), acc.getFrozenAmountChange(),
                acc.getType(), acc.getTime());
    }

    @Override
    public void onPrivateDeals(String symbol, PrivateDealsV3Api deals, long ts) {
        var d = deals; // в protobuf здесь одна сделка
        log.info("💼 DEAL {} px={} qty={} amt={} side={} fee={} {} tradeId={}",
                symbol, d.getPrice(), d.getQuantity(), d.getAmount(), d.getTradeType(),
                d.getFeeAmount(), d.getFeeCurrency(), d.getTradeId());
    }

    @Override
    public void onPrivateOrders(String symbol, PrivateOrdersV3Api o, long ts) {
        log.info("📜 ORDER {} id={} type={} side={} px={} qty={} filledQty={} avgPx={} status={} t={}",
                symbol, o.getClientId(), o.getOrderType(), o.getTradeType(),
                o.getPrice(), o.getQuantity(),
                o.getCumulativeQuantity(), o.getAvgPrice(), o.getStatus(), o.getCreateTime());
    }
}


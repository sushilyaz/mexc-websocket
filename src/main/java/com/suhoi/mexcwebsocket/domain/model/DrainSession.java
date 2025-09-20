package com.suhoi.mexcwebsocket.domain.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class DrainSession {

    public enum State {
        IDLE,
        A_MKT_BUY_DONE,
        A_SELL_PLACED,
        B_MKT_BUY_SENT,
        A_SELL_FILLED,
        A_BUY_PLACED,
        B_MKT_SELL_SENT,
        A_BUY_FILLED,
        AUTO_PAUSE
    }

    public enum AutoPauseReason {
        BALANCE_MISMATCH,
        BALANCE_STALE,
        MANUAL,
        FRONT_RUN,
        TIMEOUT,
        PARTIAL_MISMATCH,
        SPREAD_TOO_THIN,
        INSUFFICIENT_BALANCE,
        UNKNOWN
    }

    private State state = State.IDLE;
    private AutoPauseReason reason = null;
    private String reasonDetails = null;

    private String symbol;
    private int cycleIndex;

    /** Эпоха выполнения. Любая пауза/рестарт увеличивает runId, а все старые хэндлеры становятся протухшими. */
    private long runId = 0L;

    public BigDecimal qtyA;           // рабочее количество на входе цикла
    public String sellOrderId;
    public String buyOrderId;
    private BigDecimal bBaseBeforeSell = BigDecimal.ZERO;

    private BigDecimal targetDrainUSDT;
    private BigDecimal drainedUSDT = BigDecimal.ZERO;

    private BigDecimal lastSpentAUpper = BigDecimal.ZERO;

    public BigDecimal pSell;
    public BigDecimal pBuy;

    public BigDecimal lastSpentB;
    public BigDecimal lastCummA;

    private BigDecimal plannedSellQtyB = BigDecimal.ZERO;

    public int requotesSell = 0;
    public int requotesBuy  = 0;

    public long tCreated = System.currentTimeMillis();
    public long tLastUpdate = System.currentTimeMillis();

    // ===== WS-балансы =====
    private BigDecimal aBaseFree   = BigDecimal.ZERO, aBaseLocked   = BigDecimal.ZERO;
    private BigDecimal aUsdtFree   = BigDecimal.ZERO, aUsdtLocked   = BigDecimal.ZERO;
    private BigDecimal bBaseFree   = BigDecimal.ZERO, bBaseLocked   = BigDecimal.ZERO;
    private BigDecimal bUsdtFree   = BigDecimal.ZERO, bUsdtLocked   = BigDecimal.ZERO;
    private long aAccTs = 0L, bAccTs = 0L;

    private BigDecimal lastFilledLowerQty = BigDecimal.ZERO; // qty SELL[A]
    private BigDecimal lastFilledUpperQty = BigDecimal.ZERO; // qty BUY[A]

    public BigDecimal aBaseTotal() { return nz(aBaseFree).add(nz(aBaseLocked)); }
    public BigDecimal aUsdtTotal() { return nz(aUsdtFree).add(nz(aUsdtLocked)); }
    public BigDecimal bBaseTotal() { return nz(bBaseFree).add(nz(bBaseLocked)); }
    public BigDecimal bUsdtTotal() { return nz(bUsdtFree).add(nz(bUsdtLocked)); }

    public void setState(State s) {
        this.state = s;
        this.tLastUpdate = System.currentTimeMillis();
    }

    /** Любая автопауза сдвигает эпоху — старые подписки больше не должны ничего делать. */
    public void autoPause(AutoPauseReason r, String details) {
        this.state = State.AUTO_PAUSE;
        this.reason = r;
        this.reasonDetails = details;
        this.tLastUpdate = System.currentTimeMillis();
        this.runId++; // <— важное изменение
    }

    private static BigDecimal nz(BigDecimal x) { return x == null ? BigDecimal.ZERO : x; }
}

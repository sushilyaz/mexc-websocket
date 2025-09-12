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
        BALANCE_MISMATCH,     // факты ≠ ожидания
        BALANCE_STALE,        // не удалось получить/подтвердить балансы
        MANUAL,
        FRONT_RUN,            // вклинивание ММ
        TIMEOUT,              // таймаут ожидания FILLED
        PARTIAL_MISMATCH,     // факты/ожидания не сошлись
        SPREAD_TOO_THIN,      // спред слишком мал
        INSUFFICIENT_BALANCE, // не хватает средств для шага
        UNKNOWN
    }

    private State state = State.IDLE;
    private AutoPauseReason reason = null;
    private String reasonDetails = null;

    private String symbol;
    private int cycleIndex;

    public BigDecimal qtyA;           // «рабочее» кол-во базового на A на входе цикла
    public String sellOrderId;
    public String buyOrderId;
    private BigDecimal bBaseBeforeSell = BigDecimal.ZERO;

    // цель и прогресс перелива (в USDT)
    private BigDecimal targetDrainUSDT;              // сколько хотим перелить всего
    private BigDecimal drainedUSDT = BigDecimal.ZERO;// сколько уже перелили суммарно

    // сколько A потратил на BUY на верхней кромке в текущем цикле
    private BigDecimal lastSpentAUpper = BigDecimal.ZERO;

    public BigDecimal pSell;          // выставленная цена SELL (нижняя кромка)
    public BigDecimal pBuy;           // выставленная цена BUY  (верхняя кромка)

    public BigDecimal lastSpentB;     // сколько USDT реально списали с B при MARKET BUY
    public BigDecimal lastCummA;      // сколько USDT реально пришло на A при SELL

    /**
     * Сколько планируем продать с аккаунта B в текущем цикле —
     * всегда равно фактическому количеству в лимитной заявке BUY на аккаунте A.
     * Нужно для корректной сверки: остаток на B после продажи может быть НЕ пылью.
     */
    private BigDecimal plannedSellQtyB = BigDecimal.ZERO;

    // счётчики «перестановок» против вклинивания
    public int requotesSell = 0;
    public int requotesBuy  = 0;

    // таймстемпы для диагностики
    public long tCreated = System.currentTimeMillis();
    public long tLastUpdate = System.currentTimeMillis();

    // ===== WS-балансы (живые значения free/frozen) =====
    private BigDecimal aBaseFree   = BigDecimal.ZERO, aBaseLocked   = BigDecimal.ZERO;
    private BigDecimal aUsdtFree   = BigDecimal.ZERO, aUsdtLocked   = BigDecimal.ZERO;
    private BigDecimal bBaseFree   = BigDecimal.ZERO, bBaseLocked   = BigDecimal.ZERO;
    private BigDecimal bUsdtFree   = BigDecimal.ZERO, bUsdtLocked   = BigDecimal.ZERO;
    private long aAccTs = 0L, bAccTs = 0L; // времена последнего аккаунт-ивента

    // Для точной сверки FILLED-количеств
    private BigDecimal lastFilledLowerQty = BigDecimal.ZERO; // qty SELL[A]
    private BigDecimal lastFilledUpperQty = BigDecimal.ZERO; // qty BUY[A]

    // Удобные суммы
    public BigDecimal aBaseTotal() { return nz(aBaseFree).add(nz(aBaseLocked)); }
    public BigDecimal aUsdtTotal() { return nz(aUsdtFree).add(nz(aUsdtLocked)); }
    public BigDecimal bBaseTotal() { return nz(bBaseFree).add(nz(bBaseLocked)); }
    public BigDecimal bUsdtTotal() { return nz(bUsdtFree).add(nz(bUsdtLocked)); }

    public void setState(State s) {
        this.state = s;
        this.tLastUpdate = System.currentTimeMillis();
    }

    public void autoPause(AutoPauseReason r, String details) {
        this.state = State.AUTO_PAUSE;
        this.reason = r;
        this.reasonDetails = details;
        this.tLastUpdate = System.currentTimeMillis();
    }

    private static BigDecimal nz(BigDecimal x) { return x == null ? BigDecimal.ZERO : x; }
}

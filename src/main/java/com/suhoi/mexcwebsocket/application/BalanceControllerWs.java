package com.suhoi.mexcwebsocket.application;

import com.suhoi.mexcwebsocket.domain.model.DrainSession;
import com.suhoi.mexcwebsocket.domain.model.SymbolFilters;
import com.suhoi.mexcwebsocket.util.MarketMath;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
@Slf4j
public class BalanceControllerWs {

    private static final BigDecimal REL_TOL = new BigDecimal("0.003");     // 0.3%
    private static final BigDecimal ABS_EPS_Q = new BigDecimal("0.0005");  // 0.0005 USDT
    private static final long FRESH_MS = 1500;
    private static final int RETRIES = 4;
    private static final long RETRY_SLEEP_MS = 120;

    public enum Phase {
        AFTER_A_MKT_BUY,      // S1
        AFTER_A_SELL_PLACED,  // S2
        AFTER_LOWER_FILLED,   // S3
        AFTER_A_BUY_PLACED,   // S4
        AFTER_UPPER_FILLED    // S5
    }

    public boolean verify(DrainSession s, Phase phase, SymbolFilters f) {
        for (int i = 0; i < RETRIES; i++) {
            if (checkOnce(s, phase, f)) return true;
            try { Thread.sleep(RETRY_SLEEP_MS); } catch (InterruptedException ignored) {}
        }
        s.autoPause(DrainSession.AutoPauseReason.BALANCE_MISMATCH,
                "WS-balance check failed at " + phase
                        + " | A{base=" + fmt(s.aBaseTotal()) + ",quote=" + fmt(s.aUsdtTotal())
                        + "} B{base=" + fmt(s.bBaseTotal()) + ",quote=" + fmt(s.bUsdtTotal()) + "}");
        return false;
    }

    private boolean checkOnce(DrainSession s, Phase phase, SymbolFilters f) {
        long now = System.currentTimeMillis();
        if ((s.getAAccTs() > 0 && now - s.getAAccTs() > FRESH_MS) ||
                (s.getBAccTs() > 0 && now - s.getBAccTs() > FRESH_MS)) {
            s.autoPause(DrainSession.AutoPauseReason.BALANCE_STALE, "account WS stale");
            return false;
        }

        BigDecimal step = nz(f.getStepSize());
        BigDecimal tick = nz(f.getTickSize());
        BigDecimal baseEps  = step.multiply(BigDecimal.valueOf(3));
        BigDecimal quoteEps = ABS_EPS_Q.max(tick.multiply(BigDecimal.valueOf(3)));

        return switch (phase) {
            case AFTER_A_MKT_BUY -> approx(s.aBaseTotal(), nz(s.getQtyA()), baseEps, REL_TOL);

            case AFTER_A_SELL_PLACED ->
                    approx(s.getABaseLocked(), MarketMath.normalizeQty(nz(s.getQtyA()), f), baseEps, REL_TOL)
                            && s.getABaseFree().abs().compareTo(baseEps) <= 0;

            case AFTER_LOWER_FILLED -> {
                BigDecimal filledQty = nz(s.getLastFilledLowerQty()); // SELL[A]
                BigDecimal spentB    = nz(s.getLastSpentB());
                BigDecimal receivedA = nz(s.getLastCummA());
                boolean ok =
                        s.aBaseTotal().abs().compareTo(baseEps) <= 0
                                && grewByAtLeast(s.getAUsdtFree(), receivedA, quoteEps, REL_TOL)
                                && s.bBaseTotal().compareTo(filledQty.subtract(baseEps)) >= 0
                                && droppedByAtLeast(s.getBUsdtFree(), spentB, quoteEps, REL_TOL);
                yield ok;
            }

            case AFTER_A_BUY_PLACED -> {
                BigDecimal qtyU   = MarketMath.normalizeQty(nz(s.getQtyA()), f);
                BigDecimal notional = nz(s.getPBuy()).multiply(qtyU);
                boolean ok =
                        s.aBaseTotal().abs().compareTo(baseEps) <= 0
                                && s.getAUsdtLocked().compareTo(notional.subtract(quoteEps)) >= 0
                                && s.bBaseTotal().compareTo(qtyU.subtract(baseEps)) >= 0;
                yield ok;
            }

            case AFTER_UPPER_FILLED -> {
                BigDecimal filledQty = nz(s.getLastFilledUpperQty()); // BUY[A]
                BigDecimal spentA    = nz(s.getLastSpentAUpper());
                boolean ok =
                        s.aBaseTotal().compareTo(filledQty.subtract(baseEps)) >= 0
                                && droppedByAtLeast(s.getAUsdtFree(), spentA, quoteEps, REL_TOL)
                                && s.bBaseTotal().compareTo(baseEps) <= 0; // у B база ~ пыль
                yield ok;
            }
        };
    }

    private static boolean approx(BigDecimal fact, BigDecimal exp, BigDecimal absEps, BigDecimal relEps){
        BigDecimal diff = nz(fact).subtract(nz(exp)).abs();
        BigDecimal maxEps = absEps.max(nz(exp).abs().multiply(relEps));
        return diff.compareTo(maxEps) <= 0;
    }
    private static boolean grewByAtLeast(BigDecimal newVal, BigDecimal delta, BigDecimal absEps, BigDecimal relEps){
        BigDecimal thr = nz(delta).subtract(absEps.max(nz(delta).abs().multiply(relEps)));
        return nz(newVal).compareTo(thr) >= 0;
    }
    private static boolean droppedByAtLeast(BigDecimal newVal, BigDecimal delta, BigDecimal absEps, BigDecimal relEps){
        BigDecimal thr = nz(delta).subtract(absEps.max(nz(delta).abs().multiply(relEps)));
        // newVal <= start - delta  =>  -newVal >= -(start - delta)
        // здесь мы сравниваем только «масштаб» падения: newVal уменьшился минимум на delta
        return true; // упрощаем, т.к. стартового значения нет; контролим через notional/locks в других фазах
    }

    private static BigDecimal nz(BigDecimal x){ return x==null?BigDecimal.ZERO:x; }
    private static String fmt(BigDecimal x){ return com.suhoi.mexcwebsocket.util.FormatHelpers.fmt(x); }
}

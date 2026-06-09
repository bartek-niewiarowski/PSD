package pl.project;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnomalyDetectorJobTest {

    @Test
    void shouldFireLimitExceededAlert() {
        AnomalyDetectorJob.AnomalyDecision decision = AnomalyDetectorJob.decideAnomaly(
                1200.0,
                1000.0,
                2L,
                300.0,
                20.0,
                true
        );

        assertTrue(decision.isAnomaly());
        assertEquals("LIMIT_EXCEEDED", decision.anomalyType);
        assertEquals("Transaction amount exceeds available limit", decision.reason);
    }

    @Test
    void shouldFireStatisticalAmountAlertAfterEnoughHistory() {
        AnomalyDetectorJob.AnomalyDecision decision = AnomalyDetectorJob.decideAnomaly(
                120.0,
                1000.0,
                5L,
                100.0,
                100.0,
                true
        );

        assertTrue(decision.isAnomaly());
        assertEquals("STATISTICAL_AMOUNT_ANOMALY", decision.anomalyType);
        assertEquals(
                "Transaction amount significantly differs from historical average",
                decision.reason
        );
    }

    @Test
    void shouldFireNewLocationAlertAfterEnoughHistory() {
        AnomalyDetectorJob.AnomalyDecision decision = AnomalyDetectorJob.decideAnomaly(
                120.0,
                1000.0,
                5L,
                100.0,
                100.0,
                false
        );

        assertTrue(decision.isAnomaly());
        assertEquals("NEW_LOCATION", decision.anomalyType);
        assertEquals("Transaction from previously unseen location", decision.reason);
    }

    @Test
    void shouldPrioritizeNewLocationOverOtherAlertsWhenConditionsOverlap() {
        AnomalyDetectorJob.AnomalyDecision decision = AnomalyDetectorJob.decideAnomaly(
                1200.0,
                1000.0,
                5L,
                100.0,
                100.0,
                false
        );

        assertTrue(decision.isAnomaly());
        assertEquals("NEW_LOCATION", decision.anomalyType);
        assertEquals("Transaction from previously unseen location", decision.reason);
    }

    @Test
    void shouldNotFireProfileBasedAlertsBeforeMinimumHistory() {
        AnomalyDetectorJob.AnomalyDecision decision = AnomalyDetectorJob.decideAnomaly(
                120.0,
                1000.0,
                4L,
                100.0,
                100.0,
                false
        );

        assertFalse(decision.isAnomaly());
    }
}

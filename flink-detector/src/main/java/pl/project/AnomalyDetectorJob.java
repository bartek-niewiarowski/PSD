package pl.project;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.UUID;

public class AnomalyDetectorJob {

    private static final String KAFKA_BOOTSTRAP = "kafka:19092";
    private static final String INPUT_TOPIC = "transactions";
    private static final String OUTPUT_TOPIC = "alerts";
    private static final long MIN_HISTORY_FOR_PROFILE_ALERTS = 5L;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        ObjectMapper mapper = new ObjectMapper();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(INPUT_TOPIC)
                .setGroupId("flink-anomaly-detector")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(OUTPUT_TOPIC)
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                )
                                .build()
                )
                .build();

        env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka transactions source"
                )
                .keyBy(message -> {
                    JsonNode node = mapper.readTree(message);
                    JsonNode cardIdNode = node.get("card_id");

                    if (cardIdNode == null
                            || cardIdNode.isNull()
                            || cardIdNode.asText().isBlank()) {
                        throw new IllegalArgumentException(
                                "Missing required field: card_id"
                        );
                    }

                    return cardIdNode.asText();
                })
                .process(new StatisticalAnomalyDetector())
                .filter(value -> value != null && !value.isBlank())
                .sinkTo(sink);

        env.execute("Card Transaction Statistical Anomaly Detector");
    }

    public static class StatisticalAnomalyDetector
            extends KeyedProcessFunction<String, String, String> {

        private transient ObjectMapper mapper;

        // liczba transakcji
        private transient ValueState<Long> countState;

        // średnia kwota
        private transient ValueState<Double> meanState;

        // pomocnicza wartość do obliczania wariancji
        private transient ValueState<Double> m2State;

        // częste lokalizacje
        private transient MapState<String, Integer> locationCountsState;

        @Override
        public void open(Configuration parameters) {

            mapper = new ObjectMapper();

            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "transaction-count",
                            Long.class
                    )
            );

            meanState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "amount-mean",
                            Double.class
                    )
            );

            m2State = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "amount-m2",
                            Double.class
                    )
            );

            locationCountsState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "location-counts",
                            String.class,
                            Integer.class
                    )
            );
        }

        @Override
        public void processElement(
                String message,
                Context context,
                Collector<String> collector
        ) throws Exception {

            JsonNode transaction = mapper.readTree(message);

            double amount =
                    transaction.path("amount").asDouble();

            double availableLimit =
                    transaction.path("available_limit").asDouble();

            String location = buildLocationKey(transaction.path("location"));

            Long count = countState.value();
            Double mean = meanState.value();
            Double m2 = m2State.value();

            if (count == null) {
                count = 0L;
            }

            if (mean == null) {
                mean = 0.0;
            }

            if (m2 == null) {
                m2 = 0.0;
            }

            boolean knownLocation =
                    locationCountsState.contains(location);
            AnomalyDecision decision = decideAnomaly(
                    amount,
                    availableLimit,
                    count,
                    mean,
                    m2,
                    knownLocation
            );

            /*
             * Aktualizacja statystyk
             */
            updateStatistics(amount, count, mean, m2);

            /*
             * Aktualizacja lokalizacji
             */
            updateLocation(location);

            /*
             * Generowanie alertu
             */
            if (decision.isAnomaly()) {

                ObjectNode alarm =
                        mapper.createObjectNode();

                alarm.put(
                        "alarm_id",
                        UUID.randomUUID().toString()
                );

                alarm.put(
                        "created_at",
                        Instant.now().toString()
                );

                alarm.put(
                        "source",
                        "flink-statistical-anomaly-detector"
                );

                alarm.put(
                        "anomaly_type",
                        decision.anomalyType
                );

                alarm.put(
                        "reason",
                        decision.reason
                );

                alarm.put(
                        "transaction_id",
                        transaction.path("transaction_id").asText()
                );

                alarm.put(
                        "card_id",
                        transaction.path("card_id").asText()
                );

                alarm.put(
                        "user_id",
                        transaction.path("user_id").asText()
                );

                alarm.set("transaction", transaction);

                collector.collect(
                        mapper.writeValueAsString(alarm)
                );
            }
        }

        /*
         * Aktualizacja średniej i wariancji
         * Algorytm Welforda
         */
        private void updateStatistics(
                double amount,
                long count,
                double mean,
                double m2
        ) throws Exception {

            long newCount = count + 1;

            double delta = amount - mean;

            double newMean =
                    mean + delta / newCount;

            double delta2 =
                    amount - newMean;

            double newM2 =
                    m2 + delta * delta2;

            countState.update(newCount);
            meanState.update(newMean);
            m2State.update(newM2);
        }

        /*
         * Budowa stabilnego klucza lokalizacji.
         * Pole "location" jest obiektem JSON {lat, lon}, więc asText() zwróciłby
         * pusty string. Klucz budujemy z zaokrąglonych współrzędnych, aby drobne
         * wahania GPS nie tworzyły nowej lokalizacji przy każdej transakcji.
         */
        private String buildLocationKey(JsonNode location) {

            if (location == null || location.isMissingNode() || location.isNull()) {
                return "UNKNOWN";
            }

            double lat = location.path("lat").asDouble();
            double lon = location.path("lon").asDouble();

            return String.format(
                    java.util.Locale.US,
                    "%.2f,%.2f",
                    lat,
                    lon
            );
        }

        /*
         * Aktualizacja częstych lokalizacji
         */
        private void updateLocation(String location)
                throws Exception {

            Integer currentCount =
                    locationCountsState.get(location);

            if (currentCount == null) {
                currentCount = 0;
            }

            locationCountsState.put(
                    location,
                    currentCount + 1
            );
        }
    }

    static AnomalyDecision decideAnomaly(
            double amount,
            double availableLimit,
            long count,
            double mean,
            double m2,
            boolean knownLocation
    ) {
        String anomalyType = null;
        String reason = null;

        /*
         * 1. LIMIT EXCEEDED
         */
        if (amount > availableLimit) {
            anomalyType = "LIMIT_EXCEEDED";
            reason = "Transaction amount exceeds available limit";
        }
        /*
         * 2. Statistical anomaly based on Z-Score
         */
        else if (count >= MIN_HISTORY_FOR_PROFILE_ALERTS) {
            double variance = m2 / (count - 1);
            double stddev = Math.sqrt(variance);

            if (stddev > 0) {
                double zScore = Math.abs((amount - mean) / stddev);

                if (zScore > 3.0) {
                    anomalyType = "STATISTICAL_AMOUNT_ANOMALY";
                    reason = "Transaction amount significantly differs from historical average";
                }
            }
        }

        /*
         * 3. New location anomaly (overrides amount-profile anomalies)
         */
        if (count >= MIN_HISTORY_FOR_PROFILE_ALERTS && !knownLocation) {
            anomalyType = "NEW_LOCATION";
            reason = "Transaction from previously unseen location";
        }

        return new AnomalyDecision(anomalyType, reason);
    }

    static final class AnomalyDecision {
        final String anomalyType;
        final String reason;

        AnomalyDecision(String anomalyType, String reason) {
            this.anomalyType = anomalyType;
            this.reason = reason;
        }

        boolean isAnomaly() {
            return anomalyType != null;
        }
    }
}
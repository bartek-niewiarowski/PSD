package pl.project;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
import java.util.UUID;

public class AnomalyDetectorJob {

    private static final String KAFKA_BOOTSTRAP = "kafka:19092";
    private static final String INPUT_TOPIC = "transactions";
    private static final String OUTPUT_TOPIC = "alerts";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka transactions source")
                .map(new SimpleAnomalyDetector())
                .filter(value -> value != null && !value.isBlank())
                .sinkTo(sink);

        env.execute("Card Transaction Anomaly Detector");
    }

    public static class SimpleAnomalyDetector extends RichMapFunction<String, String> {

        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            this.mapper = new ObjectMapper();
        }

        @Override
        public String map(String message) throws Exception {
            JsonNode transaction = mapper.readTree(message);

            double amount = transaction.path("amount").asDouble();
            double availableLimit = transaction.path("available_limit").asDouble();
            boolean simulatorMarkedAnomaly = transaction.path("is_anomaly").asBoolean(false);
            String simulatorAnomalyType = transaction.path("anomaly_type").asText("");

            String anomalyType = null;
            String reason = null;

            if (amount > availableLimit) {
                anomalyType = "LIMIT_EXCEEDED";
                reason = "Transaction amount exceeds available card limit";
            } else if (amount > 5000) {
                anomalyType = "HIGH_AMOUNT";
                reason = "Transaction amount is suspiciously high";
            } else if (simulatorMarkedAnomaly) {
                anomalyType = simulatorAnomalyType;
                reason = "Transaction was generated as anomaly by simulator";
            }

            if (anomalyType == null || anomalyType.isBlank()) {
                return null;
            }

            ObjectNode alarm = mapper.createObjectNode();
            alarm.put("alarm_id", UUID.randomUUID().toString());
            alarm.put("created_at", Instant.now().toString());
            alarm.put("source", "flink-java-anomaly-detector");
            alarm.put("anomaly_type", anomalyType);
            alarm.put("reason", reason);
            alarm.put("transaction_id", transaction.path("transaction_id").asText());
            alarm.put("card_id", transaction.path("card_id").asText());
            alarm.put("user_id", transaction.path("user_id").asText());
            alarm.set("transaction", transaction);

            return mapper.writeValueAsString(alarm);
        }
    }
}
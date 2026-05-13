FROM flink:1.20.3-scala_2.12-java17

COPY target/flink-detector-java-1.0.0.jar /opt/flink/usrlib/flink-detector.jar

CMD ["flink", "run", "-m", "flink-jobmanager:8081", "-c", "pl.project.AnomalyDetectorJob", "/opt/flink/usrlib/flink-detector.jar"]
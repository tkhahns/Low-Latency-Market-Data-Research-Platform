FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY services/stream-processor/flink/pom.xml services/stream-processor/flink/pom.xml
COPY services/stream-processor/flink/src services/stream-processor/flink/src
RUN mvn -f services/stream-processor/flink/pom.xml -DskipTests package

FROM flink:1.19.1-java17
COPY --from=build /src/services/stream-processor/flink/target/market-state-flink-job-0.1.0.jar /opt/flink/usrlib/market-state-flink-job.jar

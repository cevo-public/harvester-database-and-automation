## Build app ##

FROM openjdk:15-slim AS builder

COPY database/java/ /build/
RUN cd /build && chmod +x gradlew && ./gradlew clean build -x test && ls -l /build/*


## Run server ##

FROM openjdk:15.0.2-buster AS server
WORKDIR /app

COPY --from=builder /build/build/libs /app

ENTRYPOINT ["java", "-Xmx12g", "-jar", "/app/vineyard-1.0-SNAPSHOT.jar"]

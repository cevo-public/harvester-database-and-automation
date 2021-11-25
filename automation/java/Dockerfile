## Build app ##

FROM openjdk:15 AS builder
WORKDIR /build/

COPY database/java/ .
RUN chmod +x ./gradlew
RUN ./gradlew clean build -x test


## Run server ##

FROM openjdk:15.0.2-buster AS server
WORKDIR /app

COPY --from=builder /build/build/libs /app

ENTRYPOINT ["java", "-Xmx12g", "-jar", "/app/vineyard-1.0-SNAPSHOT.jar"]

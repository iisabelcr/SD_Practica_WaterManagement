FROM maven:3.9-eclipse-temurin-21
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q clean compile
CMD sh -c 'mvn -q exec:java -Dexec.mainClass="UA_SD.WM_Central.WM_Central" -Dexec.args="${CENTRAL_PORT} ${KAFKA_HOST} ${KAFKA_PORT}"'
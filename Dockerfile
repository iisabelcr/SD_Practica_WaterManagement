FROM maven:3.9-eclipse-temurin-21
WORKDIR /app
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q clean compile
ENV MAIN_CLASS=UA_SD.WM_Central.WM_Central
CMD sh -c 'mvn -q exec:java -Dexec.mainClass="$MAIN_CLASS" -Dexec.args="${APP_ARGS:-$CENTRAL_PORT $KAFKA_HOST $KAFKA_PORT}"'
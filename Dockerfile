# Imagen oficial de Java 21
FROM maven:3.9-eclipse-temurin-21

# Se establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Se copian todos los archivos del repositorio al contenedor
COPY . .

# Se compila el proyecto descargando las dependencias (Kafka)
RUN mvn clean compile

# Se ajusta el puerto dinámico asignado por Railway
EXPOSE ${PORT}

# Comando final para arrancar el servidor WM_Central
CMD sh -c 'mvn exec:java -Dexec.mainClass="UA_SD.WM_Central" -Dexec.args="${PORT}"'
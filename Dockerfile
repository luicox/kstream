# Usa una imagen base con Java 11
FROM eclipse-temurin:11-jdk

# Crea el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el JAR de tu aplicación (asegúrate de construirlo antes con Maven o Gradle)
COPY target/standin-kstream-1.0.jar app.jar

# Expone puertos si es necesario (por ejemplo, para JMX o health checks)
# EXPOSE 8080

# Comando para ejecutar tu app Kafka Streams
ENTRYPOINT ["java", "-jar", "app.jar"]

FROM openjdk:11.0.3-slim
ADD /target/kafka-sidecar.jar server.jar
CMD ["/bin/sh","-c","exec java ${JVM_OPTS} -Dlight-4j-config-dir=/config -Dlogback.configurationFile=/config/logback.xml ${KAFKA_OPTS} -cp /server.jar:/streamsjar/* com.networknt.server.Server"]

package com.girisenji.kafka.monitor.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.security-protocol}")
    private String kafkaSecurityProtocol;

    @Value("${kafka.sasl-jaas-config}")
    private String saslJaasConfig;

    @Value("${kafka.sasl-mechanism}")
    private String saslMechanism;

    @Value("${kafka.ssl-keystore-location}")
    private String kafkaSslKeystoreLocation;

    @Value("${kafka.ssl-keystore-password}")
    private String kafkaSslKeystorePassword;

    @Value("${kafka.ssl-truststore-location}")
    private String kafkaSslTruststoreLocation;

    @Value("${kafka.ssl-truststore-password}")
    private String kafkaSslTruststorePassword;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.key-deserializer}")
    private String kafkaKeyDeserializer;

    @Value("${kafka.consumer.value-deserializer}")
    private String kafkaValueDeserializer;

    public String getGroupId() {
        return groupId;
    }

    @Bean
    public Map<String, Object> adminConfig() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        if (StringUtils.hasLength(groupId))
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        if (StringUtils.hasLength(kafkaSecurityProtocol))
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaSecurityProtocol);
        if (StringUtils.hasLength(saslJaasConfig))
            props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        if (StringUtils.hasLength(kafkaSslKeystoreLocation))
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        if (StringUtils.hasLength(kafkaSslKeystoreLocation))
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, kafkaSslKeystoreLocation);
        if (StringUtils.hasLength(kafkaSslKeystorePassword))
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, kafkaSslKeystorePassword);
        if (StringUtils.hasLength(kafkaSslTruststoreLocation))
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaSslTruststoreLocation);
        if (StringUtils.hasLength(kafkaSslTruststorePassword))
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaSslTruststorePassword);
        if (StringUtils.hasLength(kafkaKeyDeserializer))
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaKeyDeserializer);
        if (StringUtils.hasLength(kafkaValueDeserializer))
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaValueDeserializer);
        return props;
    }
}

package com.girisenji.kafka.monitor.services;

import com.girisenji.kafka.monitor.config.KafkaConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class ConsumerLagMonitoringService {

    private final AdminClient adminClient;
    private final KafkaConfiguration kafkaConfiguration;
    private final KafkaConsumer<String, String> consumer;
    private static final Logger log = LoggerFactory.getLogger(ConsumerLagMonitoringService.class);

    @Autowired
    public ConsumerLagMonitoringService(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
        this.adminClient = AdminClient.create(kafkaConfiguration.adminConfig());
        this.consumer = new KafkaConsumer<>(kafkaConfiguration.adminConfig());
    }

    @Scheduled(fixedDelay = 30000L)
    public void monitorLag() throws ExecutionException, InterruptedException {

        Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(this.kafkaConfiguration.getGroupId());
        Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
        Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);

        for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
            String topic = lagEntry.getKey().topic();
            int partition = lagEntry.getKey().partition();
            Long lag = lagEntry.getValue();
            log.info("Topic = {}, Group-Id = {}, Partition = {}, {}, Lag = {}", topic, this.kafkaConfiguration.getGroupId(), partition, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), lag);
        }
    }

    public Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId) throws ExecutionException, InterruptedException {

        Map<TopicPartition, OffsetAndMetadata> metadataMap = this.adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, Long> groupOffset = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : metadataMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndMetadata metadata = entry.getValue();
            groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
        }
        return groupOffset;
    }

    private Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {

        List<TopicPartition> topicPartitions = new LinkedList<>();

        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
            TopicPartition key = entry.getKey();
            topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
        }
        return consumer.endOffsets(topicPartitions);
    }

    public Map<TopicPartition, Long> computeLags(Map<TopicPartition, Long> consumerGrpOffsets, Map<TopicPartition, Long> producerOffsets) {

        Map<TopicPartition, Long> lags = new HashMap<>();

        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(Math.max(0, producerOffset) - Math.max(0, consumerOffset));
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
    }
}

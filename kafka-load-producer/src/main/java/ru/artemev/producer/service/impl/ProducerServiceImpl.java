package ru.artemev.producer.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.artemev.producer.model.ClientModel;
import ru.artemev.producer.service.ProducerService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerServiceImpl implements ProducerService {

  @Value("${json.file.path}")
  private Path filePathJson;

  @Value("${kafka.topic}")
  private String kafkaTopic;

  private final KafkaTemplate<String, ClientModel> kafkaTemplate;
  private final ObjectMapper objectMapper;

  @Override
  @EventListener(ApplicationReadyEvent.class)
  public void addFileToTopic() throws IOException {
    log.info("=== Start addFileToTopic ===");

    log.info("Get json file path " + filePathJson);

    if(!filePathJson.toFile().exists()) {
      throw new RuntimeException("File path is not exists");
    }

    Arrays.stream(objectMapper.readValue(new File(filePathJson.toString()), ClientModel[].class))
        .forEach(
            line -> {
              kafkaTemplate.send(kafkaTopic, UUID.randomUUID().toString(), line);
              log.info("Sent " + line + " to kafka");
            });

    log.info("=== Finish addFileToTopic ===");
  }
}

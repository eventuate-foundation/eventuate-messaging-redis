package io.eventuate.messaging.redis.spring.producer;

import io.eventuate.messaging.redis.spring.common.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class EventuateRedisProducer {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private StringRedisTemplate redisTemplate;
  private int partitions;

  public EventuateRedisProducer(StringRedisTemplate redisTemplate, int partitions) {
    this.redisTemplate = redisTemplate;
    this.partitions = partitions;
  }

  public CompletableFuture<?> send(String topic, String key, String body) {
    int partition = Math.abs(key.hashCode()) % partitions;

    logger.info("Sending message = {} with key = {} for topic = {}, partition = {}", body, key, topic, partition);

    redisTemplate
            .opsForStream()
            .add(StreamRecords
                    .string(Collections.singletonMap(key, body))
                    .withStreamKey(RedisUtil.channelToRedisStream(topic, partition)));

    logger.info("message sent = {} with key = {} for topic = {}, partition = {}", body, key, topic, partition);

    return CompletableFuture.completedFuture(null);
  }

  public void close() {
  }
}

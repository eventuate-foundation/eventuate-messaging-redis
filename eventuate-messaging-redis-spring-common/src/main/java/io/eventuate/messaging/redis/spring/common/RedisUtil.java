package io.eventuate.messaging.redis.spring.common;

public class RedisUtil {
  public static String channelToRedisStream(String topic, int partition) {
    return "eventuate-tram:channel:%s-%s".formatted(topic, partition);
  }
}

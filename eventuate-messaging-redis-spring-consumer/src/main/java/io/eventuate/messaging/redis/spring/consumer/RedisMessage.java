package io.eventuate.messaging.redis.spring.consumer;

public class RedisMessage {
  private String payload;

  public RedisMessage(String payload) {
    this.payload = payload;
  }

  public String getPayload() {
    return payload;
  }
}

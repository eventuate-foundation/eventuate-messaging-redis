package io.eventuate.messaging.redis.spring.consumer;

import io.eventuate.messaging.redis.spring.common.RedisConfigurationProperties;
import io.eventuate.messaging.redis.spring.common.RedisUtil;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = MessageConsumerRedisConfiguration.class)
public class MessageConsumerRedisImplTest {

  @Autowired
  private RedisTemplate<String, String> redisTemplate;

  @Autowired
  private RedisConfigurationProperties redisConfigurationProperties;

  @Autowired
  private MessageConsumerRedisImpl messageConsumer;

  @Test
  public void testMessageReceived() {
    TestInfo testInfo = new TestInfo();

    List<RedisMessage> messages = Collections.synchronizedList(new ArrayList<>());

    messageConsumer.subscribe(testInfo.getSubscriberId(), Collections.singleton(testInfo.getChannel()), messages::add);

    sendMessage(testInfo.getKey(), testInfo.getMessage(), testInfo.getChannel());

    waitForMessage(messages, testInfo.getMessage());
  }

  @Test
  public void testMessageReceivedWhenConsumerGroupExists() {
    TestInfo testInfo = new TestInfo();

    for (int i = 0; i < redisConfigurationProperties.getPartitions(); i++) {
      sendMessage(testInfo.getKey(), testInfo.getMessage(), testInfo.getChannel(), i);
      redisTemplate.opsForStream().createGroup(RedisUtil.channelToRedisStream(testInfo.getChannel(), i), ReadOffset.from("0"), testInfo.getSubscriberId());
    }

    List<RedisMessage> messages = Collections.synchronizedList(new ArrayList<>());

    messageConsumer.subscribe(testInfo.getSubscriberId(), Collections.singleton(testInfo.getChannel()), messages::add);

    Eventually.eventually(() -> {
      Assertions.assertEquals(redisConfigurationProperties.getPartitions(), messages.size());
      Assertions.assertEquals(testInfo.getMessage(), messages.get(0).getPayload());
      Assertions.assertEquals(testInfo.getMessage(), messages.get(1).getPayload());
    });
  }

  @Test
  public void testReceivingPendingMessageAfterRestart() {
    TestInfo testInfo = new TestInfo();

    List<RedisMessage> messages = Collections.synchronizedList(new ArrayList<>());

    Subscription messageSubscription = messageConsumer.subscribe(testInfo.getSubscriberId(), Collections.singleton(testInfo.getChannel()), message -> {
      messages.add(message);
      throw new RuntimeException("Something happened!");
    });

    sendMessage(testInfo.getKey(), testInfo.getMessage(), testInfo.getChannel());

    Eventually.eventually(() -> {
      Assertions.assertEquals(1, messages.size());
      Assertions.assertEquals(testInfo.getMessage(), messages.get(0).getPayload());
    });

    messages.clear();

    messageSubscription.close();

    messageConsumer.subscribe(testInfo.getSubscriberId(), Collections.singleton(testInfo.getChannel()), messages::add);

    waitForMessage(messages, testInfo.getMessage());
  }

  @Test
  public void testThatProcessingStoppedOnException() {
    TestInfo testInfo = new TestInfo();

    List<RedisMessage> messages = Collections.synchronizedList(new ArrayList<>());

    messageConsumer.subscribe(testInfo.getSubscriberId(), Collections.singleton(testInfo.getChannel()), message -> {
      if (messages.isEmpty()) {
        messages.add(message);
        throw new RuntimeException("Something happened!");
      }
      messages.add(message);
    });

    sendMessage(testInfo.getKey(), testInfo.getMessage(), testInfo.getChannel());

    Eventually.eventually(() ->
      Assertions.assertEquals(1, messages.size()));

    messageConsumer.close();
  }

  private void waitForMessage(List<RedisMessage> messages, String message) {
    Eventually.eventually(60, 500, TimeUnit.MILLISECONDS,() -> {
      Assertions.assertEquals(1, messages.size());
      Assertions.assertEquals(message, messages.get(0).getPayload());
    });
  }

  private void sendMessage(String key, String message, String channel) {
    int partition = Math.abs(key.hashCode()) % redisConfigurationProperties.getPartitions();

    sendMessage(key, message, channel, partition);
  }

  private void sendMessage(String key, String message, String channel, int partition) {
    redisTemplate
            .opsForStream()
            .add(StreamRecords
                    .string(Collections.singletonMap(key, message))
                    .withStreamKey(RedisUtil.channelToRedisStream(channel, partition)));
  }


  private static class TestInfo {
    private String subscriberId = "subscriber" + System.nanoTime();
    private String channel = "channel" + System.nanoTime();

    private String key = "key1";
    private String message = "message1";

    public String getSubscriberId() {
      return subscriberId;
    }

    public void setSubscriberId(String subscriberId) {
      this.subscriberId = subscriberId;
    }

    public String getChannel() {
      return channel;
    }

    public void setChannel(String channel) {
      this.channel = channel;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }
}

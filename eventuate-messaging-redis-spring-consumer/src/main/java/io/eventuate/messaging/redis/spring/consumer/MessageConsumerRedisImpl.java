package io.eventuate.messaging.redis.spring.consumer;

import io.eventuate.messaging.partitionmanagement.CommonMessageConsumer;
import io.eventuate.messaging.partitionmanagement.CoordinatorFactory;
import io.eventuate.messaging.partitionmanagement.SubscriptionLeaderHook;
import io.eventuate.messaging.partitionmanagement.SubscriptionLifecycleHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public class MessageConsumerRedisImpl implements CommonMessageConsumer {

  private Logger logger = LoggerFactory.getLogger(getClass());

  public final String consumerId;
  private Supplier<String> subscriptionIdSupplier;

  private RedisTemplate<String, String> redisTemplate;

  private ConcurrentLinkedQueue<Subscription> subscriptions = new ConcurrentLinkedQueue<>();
  private final CoordinatorFactory coordinatorFactory;
  private long timeInMillisecondsToSleepWhenKeyDoesNotExist;
  private long blockStreamTimeInMilliseconds;

  public MessageConsumerRedisImpl(RedisTemplate<String, String> redisTemplate,
                                  CoordinatorFactory coordinatorFactory,
                                  long timeInMillisecondsToSleepWhenKeyDoesNotExist,
                                  long blockStreamTimeInMilliseconds) {
    this(() -> UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            redisTemplate,
            coordinatorFactory,
            timeInMillisecondsToSleepWhenKeyDoesNotExist,
            blockStreamTimeInMilliseconds);
  }

  public MessageConsumerRedisImpl(Supplier<String> subscriptionIdSupplier,
                                  String consumerId,
                                  RedisTemplate<String, String> redisTemplate,
                                  CoordinatorFactory coordinatorFactory,
                                  long timeInMillisecondsToSleepWhenKeyDoesNotExist,
                                  long blockStreamTimeInMilliseconds) {

    this.subscriptionIdSupplier = subscriptionIdSupplier;
    this.consumerId = consumerId;
    this.redisTemplate = redisTemplate;
    this.coordinatorFactory = coordinatorFactory;
    this.timeInMillisecondsToSleepWhenKeyDoesNotExist = timeInMillisecondsToSleepWhenKeyDoesNotExist;
    this.blockStreamTimeInMilliseconds = blockStreamTimeInMilliseconds;

    logger.info("Consumer created (consumer id = {})", consumerId);
  }

  public Subscription subscribe(String subscriberId, Set<String> channels, RedisMessageHandler handler) {

    logger.info("Consumer subscribes to channels (consumer id = {}, subscriber id {}, channels = {})", consumerId, subscriberId, channels);

    Subscription subscription = new Subscription(subscriptionIdSupplier.get(),
            consumerId,
            redisTemplate,
            subscriberId,
            channels,
            handler,
            coordinatorFactory,
            timeInMillisecondsToSleepWhenKeyDoesNotExist,
            blockStreamTimeInMilliseconds);

    subscriptions.add(subscription);

    subscription.setClosingCallback(() -> subscriptions.remove(subscription));

    logger.info("Consumer subscribed to channels (consumer id = {}, subscriber id {}, channels = {})", consumerId, subscriberId, channels);

    return subscription;
  }

  public void setSubscriptionLifecycleHook(SubscriptionLifecycleHook subscriptionLifecycleHook) {
    subscriptions.forEach(subscription -> subscription.setSubscriptionLifecycleHook(subscriptionLifecycleHook));
  }

  public void setLeaderHook(SubscriptionLeaderHook leaderHook) {
    subscriptions.forEach(subscription -> subscription.setLeaderHook(leaderHook));
  }

  @Override
  public void close() {
    logger.info("Closing consumer (consumer id = {})", consumerId);
    subscriptions.forEach(Subscription::close);
    subscriptions.clear();
    logger.info("Closed consumer (consumer id = {})", consumerId);
  }

  public String getId() {
    return consumerId;
  }
}
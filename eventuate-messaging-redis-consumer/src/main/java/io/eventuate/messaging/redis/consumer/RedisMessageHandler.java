package io.eventuate.messaging.redis.consumer;

import java.util.function.Consumer;

public interface RedisMessageHandler extends Consumer<RedisMessage> {
}

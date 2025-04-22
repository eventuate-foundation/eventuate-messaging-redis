package io.eventuate.messaging.redis.spring.common;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class CommonRedisConfiguration {

  @Bean
  public RedisConfigurationProperties redisConfigurationProperties() {
    return new RedisConfigurationProperties();
  }

  @Bean
  public RedisServers redisServers(RedisConfigurationProperties redisConfigurationProperties) {
    return new RedisServers(redisConfigurationProperties.getServers());
  }

  // @Bean Avoid conflicts with org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.
  public LettuceConnectionFactory lettuceConnectionFactory(RedisServers redisServers) {
    RedisServers.HostAndPort mainServer = redisServers.getHostsAndPorts().get(0);
    LettuceConnectionFactory factory = new LettuceConnectionFactory( mainServer.getHost( ), mainServer.getPort( ) );
    factory.afterPropertiesSet();
    return factory;
  }

  @Bean
  public EventuateRedisTemplate eventuateRedisTemplate( RedisServers redisServers) {
    StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
    EventuateRedisTemplate template = new EventuateRedisTemplate();
    template.setConnectionFactory(lettuceConnectionFactory(redisServers));
    template.setDefaultSerializer(stringRedisSerializer);
    template.setKeySerializer(stringRedisSerializer);
    template.setValueSerializer(stringRedisSerializer);
    template.setHashKeySerializer(stringRedisSerializer);
    return template;
  }

  @Bean
  public RedissonClients redissonClients(RedisServers redisServers) {
    return new RedissonClients(redisServers);
  }
}

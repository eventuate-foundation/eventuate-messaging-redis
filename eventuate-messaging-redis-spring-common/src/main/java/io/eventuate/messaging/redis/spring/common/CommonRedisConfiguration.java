package io.eventuate.messaging.redis.spring.common;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
@EnableAutoConfiguration
public class CommonRedisConfiguration {

  @Bean
  public RedisConfigurationProperties redisConfigurationProperties() {
    return new RedisConfigurationProperties();
  }

  @Bean
  public RedisServers redisServers(RedisConfigurationProperties redisConfigurationProperties) {
    return new RedisServers(redisConfigurationProperties.getServers());
  }

  @Bean
  public LettuceConnectionFactory lettuceConnectionFactory(RedisServers redisServers) {
    RedisServers.HostAndPort mainServer = redisServers.getHostsAndPorts().get(0);
    return new LettuceConnectionFactory(mainServer.getHost(), mainServer.getPort());
  }


  @Bean
  public RedissonClients redissonClients(RedisServers redisServers) {
    return new RedissonClients(redisServers);
  }
}

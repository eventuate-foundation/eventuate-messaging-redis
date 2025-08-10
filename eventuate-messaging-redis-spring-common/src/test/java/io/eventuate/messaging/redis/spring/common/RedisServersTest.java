package io.eventuate.messaging.redis.spring.common;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RedisServersTest {

  @Test
  public void testParsingOfSingleServer() {
    String servers = "somehost:123";

    RedisServers redisServers = new RedisServers(servers);

    Assertions.assertEquals(redisServers.getHostsAndPorts(),
            ImmutableList.of(new RedisServers.HostAndPort("somehost", 123)));
  }

  @Test
  public void testParsingOfSeveralServers() {
    String servers = "host1:1,host2:2,host3:3";

    RedisServers redisServers = new RedisServers(servers);

    Assertions.assertEquals(redisServers.getHostsAndPorts(),
            ImmutableList.of(new RedisServers.HostAndPort("host1", 1),
                    new RedisServers.HostAndPort("host2", 2),
                    new RedisServers.HostAndPort("host3", 3)));
  }

  @Test
  public void testParsingOfSeveralServersWithExtraSpaces() {
    String servers = " host1:1 ,  host2:2,  host3:3 ";

    RedisServers redisServers = new RedisServers(servers);

    Assertions.assertEquals(redisServers.getHostsAndPorts(),
            ImmutableList.of(new RedisServers.HostAndPort("host1", 1),
                    new RedisServers.HostAndPort("host2", 2),
                    new RedisServers.HostAndPort("host3", 3)));
  }
}

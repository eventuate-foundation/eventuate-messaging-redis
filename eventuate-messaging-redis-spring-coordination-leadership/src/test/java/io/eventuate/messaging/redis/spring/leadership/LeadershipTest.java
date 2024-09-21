package io.eventuate.messaging.redis.spring.leadership;

import io.eventuate.coordination.leadership.LeaderSelectedCallback;
import io.eventuate.coordination.leadership.tests.AbstractLeadershipTest;
import io.eventuate.coordination.leadership.tests.SelectorUnderTest;
import io.eventuate.messaging.redis.spring.common.CommonRedisConfiguration;
import io.eventuate.messaging.redis.spring.common.RedissonClients;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = CommonRedisConfiguration.class)
public class LeadershipTest extends AbstractLeadershipTest<RedisLeaderSelector> {

  @Autowired
  private RedissonClients redissonClients;

  private String groupId;
  private String memberId;

  @Before
  public void init() {
    groupId = UUID.randomUUID().toString();
    memberId = UUID.randomUUID().toString();
  }

  @Test
  public void testThatLeaderChangedWhenExpired() {
    SelectorUnderTest<RedisLeaderSelector> leaderSelectorTestingWrap1 = createAndStartLeaderSelector();
    SelectorUnderTest<RedisLeaderSelector> leaderSelectorTestingWrap2 = createAndStartLeaderSelector();

    eventuallyAssertLeadershipIsAssignedOnlyForOneSelector(leaderSelectorTestingWrap1, leaderSelectorTestingWrap2);

    SelectorUnderTest<RedisLeaderSelector> instanceWhichBecameLeaderFirst =
            leaderSelectorTestingWrap1.isLeader() ? leaderSelectorTestingWrap1 : leaderSelectorTestingWrap2;

    SelectorUnderTest<RedisLeaderSelector> instanceWhichBecameLeaderLast =
            leaderSelectorTestingWrap2.isLeader() ? leaderSelectorTestingWrap1 : leaderSelectorTestingWrap2;

    instanceWhichBecameLeaderFirst.getSelector().stopRefreshing();
    instanceWhichBecameLeaderLast.eventuallyAssertIsLeaderAndCallbackIsInvokedOnce();

    instanceWhichBecameLeaderFirst.stop();
    instanceWhichBecameLeaderLast.stop();
  }

  @Override
  protected RedisLeaderSelector createLeaderSelector(LeaderSelectedCallback leaderSelectedCallback,
                                                     Runnable leaderRemovedCallback) {
    return new RedisLeaderSelector(redissonClients,
            String.format("some:path:%s", groupId),
            String.format("[groupId: %s, memberId: %s]", groupId, memberId),
            100,
            leaderSelectedCallback,
            leaderRemovedCallback);
  }
}

package io.eventuate.messaging.redis.spring.consumer;

import io.eventuate.messaging.partitionmanagement.Assignment;
import io.eventuate.messaging.redis.spring.common.CommonRedisConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;

@SpringBootTest(classes = CommonRedisConfiguration.class)
public class AssignmentManagingTest {

  @Autowired
  private
  StringRedisTemplate redisTemplate;

  private String groupId;
  private String memberId;
  private Assignment assignment;

  @BeforeEach
  public void init() {
    groupId = UUID.randomUUID().toString();
    memberId = UUID.randomUUID().toString();
    assignment = createAssignment();
  }

  @Test
  public void testListeningForAssignment() {
    Set<Assignment> assignments = Collections.synchronizedSet(new HashSet<>());

    RedisAssignmentListener redisAssignmentListener = new RedisAssignmentListener(redisTemplate, groupId, memberId, 50, a -> {
      Assertions.assertEquals(1, assignments.size());
      Assertions.assertTrue(assignments.contains(a));
    });

    RedisAssignmentManager redisAssignmentManager = createRedisAssignmentManager();

    redisAssignmentManager.saveAssignment(groupId, memberId, assignment);

    redisAssignmentListener.remove();
  }

  @Test
  public void testWriteReadAssignment() {
    RedisAssignmentManager redisAssignmentManager = createRedisAssignmentManager();

    redisAssignmentManager.saveAssignment(groupId, memberId, assignment);

    Assertions.assertEquals(assignment, redisAssignmentManager.readAssignment(groupId, memberId));
  }

  private Assignment createAssignment() {
    String channel = UUID.randomUUID().toString();
    return new Assignment(Collections.singleton(channel), Collections.singletonMap(channel, new HashSet<>(Arrays.asList(0, 1))));
  }

  private RedisAssignmentManager createRedisAssignmentManager() {
    return new RedisAssignmentManager(redisTemplate, 36000000);
  }
}

package io.eventuate.messaging.redis.consumer;

import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.messaging.partitionmanagement.Assignment;
import io.eventuate.messaging.partitionmanagement.AssignmentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

public class RedisAssignmentManager implements AssignmentManager {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private RedisTemplate<String, String> redisTemplate;
  private long assignmentTtlInMilliseconds;

  public RedisAssignmentManager(RedisTemplate<String, String> redisTemplate, long assignmentTtlInMilliseconds) {
    this.redisTemplate = redisTemplate;
    this.assignmentTtlInMilliseconds = assignmentTtlInMilliseconds;
  }

  @Override
  public void initializeAssignment(String groupId, String memberId, Assignment assignment) {
    String assignmentKey = RedisKeyUtil.keyForAssignment(groupId, memberId);
    logger.info("Initializing assignment: key = {}, assignment = {}", assignmentKey, assignment);
    redisTemplate.opsForValue().set(assignmentKey, JSonMapper.toJson(assignment), assignmentTtlInMilliseconds, TimeUnit.MILLISECONDS);
    logger.info("Initialized assignment: key = {}, assignment = {}", assignmentKey, assignment);
  }

  @Override
  public Assignment readAssignment(String groupId, String memberId) {
    String assignmentKey = RedisKeyUtil.keyForAssignment(groupId, memberId);
    logger.info("Reading assignment: key = {}", assignmentKey);
    Assignment assignment = JSonMapper.fromJson(redisTemplate.opsForValue().get(assignmentKey), Assignment.class);
    logger.info("Read assignment: key = {}, assignment = {}", assignmentKey, assignment);
    return assignment;
  }

  @Override
  public void saveAssignment(String groupId, String memberId, Assignment assignment) {
    String assignmentKey = RedisKeyUtil.keyForAssignment(groupId, memberId);
    logger.info("Saving assignment: key = {}, assignment = {}", assignmentKey, assignment);
    redisTemplate.opsForValue().set(assignmentKey, JSonMapper.toJson(assignment), assignmentTtlInMilliseconds, TimeUnit.MILLISECONDS);
    logger.info("Saved assignment: key = {}, assignment = {}", assignmentKey, assignment);
  }
}

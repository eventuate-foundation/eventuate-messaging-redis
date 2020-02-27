package io.eventuate.messaging.redis.consumer;

import io.eventuate.common.json.mapper.JSonMapper;
import io.eventuate.messaging.partitionmanagement.Assignment;
import io.eventuate.messaging.partitionmanagement.AssignmentListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

public class RedisAssignmentListener implements AssignmentListener {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private RedisTemplate<String, String> redisTemplate;
  private Consumer<Assignment> assignmentUpdatedCallback;
  private long assignmentListenerInterval;

  private String assignmentKey;
  private Optional<Assignment> lastAssignment;
  private Timer timer = new Timer();

  public RedisAssignmentListener(RedisTemplate<String, String> redisTemplate,
                                 String groupId,
                                 String memberId,
                                 long assignmentListenerInterval,
                                 Consumer<Assignment> assignmentUpdatedCallback) {

    this.redisTemplate = redisTemplate;
    this.assignmentListenerInterval = assignmentListenerInterval;
    this.assignmentUpdatedCallback = assignmentUpdatedCallback;

    assignmentKey = RedisKeyUtil.keyForAssignment(groupId, memberId);

    lastAssignment = readAssignment();
    lastAssignment.ifPresent(assignmentUpdatedCallback);

    scheduleAssignmentCheck();
  }

  private void scheduleAssignmentCheck() {
    logger.info("Scheduling assignment check, key = {}", assignmentKey);

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          checkAssignmentUpdate();
        } catch (Exception e) {
          logger.error("Assignment check failed, key = {}", assignmentKey);
          logger.error("Assignment check failed", e);
        }
      }
    }, 0, assignmentListenerInterval);

    logger.info("Scheduled assignment check, key = {}", assignmentKey);
  }

  private void checkAssignmentUpdate() {
    Optional<Assignment> currentAssignment = readAssignment();

    if (!currentAssignment.equals(lastAssignment)) {
      currentAssignment.ifPresent(assignmentUpdatedCallback);
      lastAssignment = currentAssignment;
    }
  }

  private Optional<Assignment> readAssignment() {
    return Optional
            .ofNullable(redisTemplate.opsForValue().get(assignmentKey))
            .map(jsonAssignment -> JSonMapper.fromJson(jsonAssignment, Assignment.class));
  }

  public void remove() {
    logger.info("Removing assignment check, key = {}", assignmentKey);
    timer.cancel();
    logger.info("Removed assignment check, key = {}", assignmentKey);
  }
}

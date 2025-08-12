package io.eventuate.messaging.redis.spring.consumer;

import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.messaging.partitionmanagement.*;
import io.eventuate.messaging.redis.spring.common.CommonRedisConfiguration;
import io.eventuate.messaging.redis.spring.common.RedisConfigurationProperties;
import io.eventuate.messaging.redis.spring.common.RedissonClients;
import io.eventuate.messaging.redis.spring.leadership.RedisLeaderSelector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
@Import(CommonRedisConfiguration.class)
public class MessageConsumerRedisConfiguration {

  @Bean
  public MessageConsumerRedisImpl messageConsumerRedis(StringRedisTemplate redisTemplate,
                                                       CoordinatorFactory coordinatorFactory,
                                                       RedisConfigurationProperties redisConfigurationProperties) {

    return new MessageConsumerRedisImpl(redisTemplate,
            coordinatorFactory,
            redisConfigurationProperties.getTimeInMillisecondsToSleepWhenKeyDoesNotExist(),
            redisConfigurationProperties.getBlockStreamTimeInMilliseconds());
  }

  @Bean
  public CoordinatorFactory redisCoordinatorFactory(AssignmentManager assignmentManager,
                                                    AssignmentListenerFactory assignmentListenerFactory,
                                                    MemberGroupManagerFactory memberGroupManagerFactory,
                                                    LeaderSelectorFactory leaderSelectorFactory,
                                                    GroupMemberFactory groupMemberFactory,
                                                    RedisConfigurationProperties redisConfigurationProperties) {
    return new CoordinatorFactoryImpl(assignmentManager,
            assignmentListenerFactory,
            memberGroupManagerFactory,
            leaderSelectorFactory,
            groupMemberFactory,
            redisConfigurationProperties.getPartitions());
  }

  @Bean
  public GroupMemberFactory groupMemberFactory(StringRedisTemplate redisTemplate,
                                               RedisConfigurationProperties redisConfigurationProperties) {
    return (groupId, memberId) ->
            new RedisGroupMember(redisTemplate,
                    groupId,
                    memberId,
                    redisConfigurationProperties.getGroupMemberTtlInMilliseconds());
  }

  @Bean
  public LeaderSelectorFactory leaderSelectorFactory(RedissonClients redissonClients,
                                                     RedisConfigurationProperties redisConfigurationProperties) {
    return (lockId, leaderId, leaderSelectedCallback, leaderRemovedCallback) ->
            new RedisLeaderSelector(redissonClients,
                    lockId,
                    leaderId,
                    redisConfigurationProperties.getLeadershipTtlInMilliseconds(),
                    leaderSelectedCallback,
                    leaderRemovedCallback);
  }

  @Bean
  public MemberGroupManagerFactory memberGroupManagerFactory(StringRedisTemplate redisTemplate,
                                                             RedisConfigurationProperties redisConfigurationProperties) {
    return (groupId, memberId, groupMembersUpdatedCallback) ->
            new RedisMemberGroupManager(redisTemplate,
                    groupId,
                    memberId,
                    redisConfigurationProperties.getListenerIntervalInMilliseconds(),
                    groupMembersUpdatedCallback);
  }

  @Bean
  public AssignmentListenerFactory assignmentListenerFactory(StringRedisTemplate redisTemplate,
                                                             RedisConfigurationProperties redisConfigurationProperties) {
    return (groupId, memberId, assignmentUpdatedCallback) ->
            new RedisAssignmentListener(redisTemplate,
                    groupId,
                    memberId,
                    redisConfigurationProperties.getListenerIntervalInMilliseconds(),
                    assignmentUpdatedCallback);
  }

  @Bean
  public AssignmentManager assignmentManager(StringRedisTemplate redisTemplate,
                                             RedisConfigurationProperties redisConfigurationProperties) {
    return new RedisAssignmentManager(redisTemplate, redisConfigurationProperties.getAssignmentTtlInMilliseconds());
  }
}

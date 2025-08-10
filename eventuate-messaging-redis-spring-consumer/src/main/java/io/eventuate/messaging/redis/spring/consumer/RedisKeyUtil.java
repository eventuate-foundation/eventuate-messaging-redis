package io.eventuate.messaging.redis.spring.consumer;

public class RedisKeyUtil {
  public static String keyForMemberGroupSet(String groupId) {
    return "eventuate-tram:group:members:%s".formatted(groupId);
  }

  public static String keyForGroupMember(String groupId, String memberId) {
    return "eventuate-tram:group:member:%s:%s".formatted(groupId, memberId);
  }

  public static String keyForAssignment(String groupId, String memberId) {
    return "eventuate-tram:group:member:assignment:%s:%s".formatted(groupId, memberId);
  }

  public static String keyForLeaderLock(String groupId) {
    return "eventuate-tram:leader:lock:%s".formatted(groupId);
  }
}

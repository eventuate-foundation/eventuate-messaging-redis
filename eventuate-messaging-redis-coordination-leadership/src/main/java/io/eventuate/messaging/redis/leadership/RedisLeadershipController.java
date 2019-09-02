package io.eventuate.messaging.redis.leadership;

import io.eventuate.coordination.leadership.LeadershipController;

import java.util.concurrent.CountDownLatch;

public class RedisLeadershipController implements LeadershipController {

  private CountDownLatch stopCountDownLatch = new CountDownLatch(1);

  public RedisLeadershipController(CountDownLatch stopCountDownLatch) {
    this.stopCountDownLatch = stopCountDownLatch;
  }

  @Override
  public void stop() {
    stopCountDownLatch.countDown();
  }
}

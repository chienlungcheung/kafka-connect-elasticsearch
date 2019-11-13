package com.hannesstockner.connect.es;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by qlzhang on 2019/11/7.
 */
public class Monitor {
  private static ScheduledExecutorService monitor;
  @Getter
  private static AtomicLong toBeSentRequests = new AtomicLong(0);
  @Getter
  private static AtomicLong successfulRequests = new AtomicLong(0);
  @Getter
  private static AtomicLong failedRequests = new AtomicLong(0);

  private static final Logger logger = LoggerFactory.getLogger(Monitor.class);

  public static void start(long initialDelay,
                           long period,
                           TimeUnit unit) {
    monitor = Executors.newSingleThreadScheduledExecutor();
    monitor.scheduleAtFixedRate(() -> logger.info("toBeSentRequests = {}, successfulRequests = {}, failedRequests = {}",
      toBeSentRequests.get(), successfulRequests.get(), failedRequests.get()), initialDelay, period, unit);
  }

  public static void stop() {
    monitor.shutdown();
  }
}

/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.query.scheduler.tokenbucket;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of Token Bucket Scheduler that schedules based on available
 * tokens.
 */
public class TokenBucketScheduler extends QueryScheduler {
  private static Logger LOGGER = LoggerFactory.getLogger(TokenBucketScheduler.class);

  public static final int MAX_THREAD_LIMIT = Math.max(1, Runtime.getRuntime().availableProcessors() / 3);
  public static final String THREADS_PER_QUERY_PCT = "threads_per_query_pct";
  private final SchedulerPriorityQueue queryQueue;
  private final AtomicInteger pendingQuries = new AtomicInteger(0);
  private final Semaphore runningQueriesSemaphore = new Semaphore(numQueryRunnerThreads);
  private final int maxThreadsPerQuery;

  public TokenBucketScheduler(@Nonnull Configuration schedulerConfig, QueryExecutor queryExecutor,
      ServerMetrics serverMetrics) {
    super(schedulerConfig, queryExecutor, serverMetrics);
    queryQueue = new PriorityQueryQueue();
    int tpqPct = schedulerConfig.getInt(THREADS_PER_QUERY_PCT, 30);
    if (tpqPct > 1 &&  tpqPct <= 100) {
      maxThreadsPerQuery = Math.max(1, numQueryWorkerThreads * tpqPct / 100);
    } else {
      LOGGER.error("Invalid value for {}, using default: {}", THREADS_PER_QUERY_PCT, MAX_THREAD_LIMIT);
      maxThreadsPerQuery = MAX_THREAD_LIMIT;
    }
  }

  @Override
  public ListenableFuture<byte[]> submit(@Nullable final QueryRequest queryRequest) {
    final BoundedAccountingExecutor executor = new BoundedAccountingExecutor(getWorkerExecutorService(),
        queryRequest.getTableName());
    ListenableFutureTask<DataTable> queryFutureTask = getQueryFutureTask(queryRequest, executor);
    ListenableFuture<byte[]> queryResultFuture = getQueryResultFuture(queryRequest, queryFutureTask);
    final SchedulerQueryContext schedQueryContext = new SchedulerQueryContext(queryRequest, queryFutureTask,
        queryResultFuture);
    schedQueryContext.setExecutor(executor);
    queryResultFuture.addListener(new Runnable() {
      @Override
      public void run() {
        queryQueue.markTaskDone(schedQueryContext);
        runningQueriesSemaphore.release();
        schedQueryContext.getTableAccountant().decrementThreads();
      }
    }, MoreExecutors.directExecutor());
    pendingQuries.incrementAndGet();
    queryQueue.put(schedQueryContext);

    return queryResultFuture;
  }

  @Override
  public void start() {
    Thread scheduler = new Thread(new Runnable() {
      @Override
      public void run() {
        while(true) {
          try {
            runningQueriesSemaphore.acquire();
          } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore. Exiting.", e);
            break;
          }
          SchedulerQueryContext request = queryQueue.take();
          BoundedAccountingExecutor executor = request.getExecutor();
          executor.setBounds(new Semaphore(maxThreadsPerQuery));
          request.getTableAccountant().incrementThreads();
          request.getExecutor().setTableAccountant(request.getTableAccountant());
          pendingQuries.decrementAndGet();
          queryRunners.submit(request.getQueryFutureTask());
        }
      }
    });
    scheduler.setName("query-scheduler");
    scheduler.setPriority(Thread.MAX_PRIORITY);
    scheduler.start();
  }
}

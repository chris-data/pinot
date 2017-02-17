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
package com.linkedin.pinot.server.request;

/**
 * A simple implementation of RequestHandler.
 */
/*
public class SimpleRequestHandler implements RequestHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRequestHandler.class);

  private ServerMetrics _serverMetrics;
  QueryExecutor _queryExecutor = null;
  private final AtomicInteger runningQueries = new AtomicInteger(0);

  public SimpleRequestHandler(QueryExecutor queryExecutor, ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _serverMetrics = serverMetrics;
  }

  @Override
  public ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext, ByteBuf request) {

    long queryStartTimeNs = System.nanoTime();
    _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);

    LOGGER.debug("processing request : {}", request);

    DataTable instanceResponse = null;

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    InstanceRequest instanceRequest = null;
    TimerContext timerContext = null;
    QueryRequest queryRequest = null;
    try {
      instanceRequest = new InstanceRequest();
      if (! serDe.deserialize(instanceRequest, byteArray) ) {
        // the deserialize method logs and suppresses exception
        LOGGER.error("Failed to deserialize query request from broker ip: {}",
           ((InetSocketAddress) channelHandlerContext.channel().remoteAddress()).getAddress().getHostAddress());
        DataTable result = new DataTableImplV2();
        result.addException(QueryException.INTERNAL_ERROR);
        _serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
        queryRequest = new QueryRequest(null, _serverMetrics);
        queryRequest.getTimerContext().setQueryArrivalTimeNs(queryStartTimeNs);

        return Futures.immediateFuture(
            ScheduledRequestHandler.serializeDataTable(queryRequest, instanceResponse));
      }
      queryRequest = new QueryRequest(instanceRequest, _serverMetrics);
      timerContext = queryRequest.getTimerContext();
      timerContext.setQueryArrivalTimeNs(queryStartTimeNs);
      TimerContext.Timer deserializeTimer =
          timerContext.startNewPhaseTimerAtNs(ServerQueryPhase.REQUEST_DESERIALIZATION, queryStartTimeNs);
      deserializeTimer.stopAndRecord();

      LOGGER.debug("Processing requestId: {},request: {}", instanceRequest.getRequestId(), instanceRequest);

      _serverMetrics.addValueToTableGauge(queryRequest.getTableName(), ServerGauge.RUNNING_QUERIES,
          runningQueries.incrementAndGet());

      instanceResponse = _queryExecutor.processQuery(queryRequest);

    } catch (Exception e) {
      LOGGER.error("Got exception while processing request. Returning error response for requestId: {}, brokerId: {}",
          instanceRequest.getRequestId(), instanceRequest.getBrokerId(), e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      instanceResponse = new DataTableImplV2();
      instanceResponse.addException(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    } finally {
      // some inconsistency is possible between the increment+record above and decrement+record below.
      // these are complex operations that are not atomic but we prefer that to synchronizing
      // recording of metrics values
      _serverMetrics.addValueToTableGauge(queryRequest.getTableName(), ServerGauge.RUNNING_QUERIES,
          runningQueries.decrementAndGet());
    }

    byte[] responseBytes = ScheduledRequestHandler.serializeDataTable(queryRequest, instanceResponse);

    ListenableFuture<byte[]> responseFuture = Futures.immediateFuture(responseBytes);
    LOGGER.info("Processed requestId {},reqSegments={},prunedToSegmentCount={},deserTimeMs={},planTimeMs={},planExecTimeMs={},totalExecMs={},serTimeMs={}TotalTimeMs={},broker={}",
        queryRequest.getInstanceRequest().getRequestId(),
        queryRequest.getInstanceRequest().getSearchSegments().size(),
        queryRequest.getSegmentCountAfterPruning(),
        timerContext.getPhaseDurationMs(ServerQueryPhase.REQUEST_DESERIALIZATION),
        timerContext.getPhaseDurationMs(ServerQueryPhase.BUILD_QUERY_PLAN),
        timerContext.getPhaseDurationMs(ServerQueryPhase.QUERY_PLAN_EXECUTION),
        timerContext.getPhaseDurationMs(ServerQueryPhase.QUERY_PROCESSING),
        timerContext.getPhaseDurationMs(ServerQueryPhase.RESPONSE_SERIALIZATION),
        timerContext.getPhaseDurationMs(ServerQueryPhase.TOTAL_QUERY_TIME),
        queryRequest.getBrokerId());

    return responseFuture;
  }

}
*/

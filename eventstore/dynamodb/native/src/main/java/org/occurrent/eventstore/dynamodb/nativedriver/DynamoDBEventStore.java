package org.occurrent.eventstore.dynamodb.nativedriver;

import io.cloudevents.CloudEvent;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.Document;
import org.occurrent.eventstore.api.LongConditionEvaluator;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper;
import org.occurrent.functionalsupport.internal.FunctionalSupport.Pair;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDBEventStore implements EventStore {
  private static final String ATTRIBUTE_STREAM = "stream";
  private static final String ATTRIBUTE_VERSION = "version";
  private static final String ATTRIBUTE_EVENTS = "events";

  private DynamoDbClient dynamoDB;
  private String tableName;
  private TimeRepresentation timeRepresentation;

  public DynamoDBEventStore(DynamoDbClient dynamoDB, String tableName, TimeRepresentation timeRepresentation) {
    this.dynamoDB = dynamoDB;
    this.tableName = tableName;
    this.timeRepresentation = timeRepresentation;
    TableDescription table = dynamoDB.describeTable(
        DescribeTableRequest.builder().tableName(tableName).build()).table();
    if (table.keySchema().stream().filter(k -> ATTRIBUTE_STREAM.equals(k)).findAny().isPresent()) {
      throw new IllegalStateException("Incorrect table");
    }
    // TODO: add more validation
  }

  public static DynamoDBEventStore create(DynamoDbClient dynamoDB, String tableName, TimeRepresentation timeRepresentation, long readCapacity, long writeCapacity) {
    try {
      dynamoDB.createTable(CreateTableRequest.builder()
          .tableName(tableName)
          .keySchema(
              KeySchemaElement.builder().attributeName(ATTRIBUTE_STREAM).keyType(KeyType.HASH)
                  .build(),
              KeySchemaElement.builder().attributeName(ATTRIBUTE_VERSION).keyType(KeyType.RANGE)
                  .build())
          .attributeDefinitions(
              AttributeDefinition.builder().attributeName(ATTRIBUTE_STREAM)
                  .attributeType(ScalarAttributeType.S).build(),
              AttributeDefinition.builder().attributeName(ATTRIBUTE_VERSION)
                  .attributeType(ScalarAttributeType.N).build())
          .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(readCapacity)
              .writeCapacityUnits(writeCapacity).build())
          .build());
    } catch (ResourceInUseException e) {
      // ignore existing table
    }
    return new DynamoDBEventStore(dynamoDB, tableName, timeRepresentation);
  }

  public void deleteTable() {
    try {
      dynamoDB.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (ResourceNotFoundException e) {
      // ignore
    }
  }

  @Override
  public WriteResult write(String streamId, Stream<CloudEvent> events) {
    return write(streamId, WriteCondition.anyStreamVersion(), events);
  }

  @Override
  public WriteResult write(String streamId, WriteCondition writeCondition,
      Stream<CloudEvent> events) {
    QueryResponse res = dynamoDB.query(QueryRequest.builder()
        .tableName(tableName)
        .attributesToGet(ATTRIBUTE_VERSION)
        .scanIndexForward(false)
        .limit(1)
        .keyConditions(Map.of(ATTRIBUTE_STREAM, Condition.builder()
            .attributeValueList(AttributeValue.builder().s(streamId).build())
            .comparisonOperator(ComparisonOperator.EQ)
            .build()))
        .build());
    AtomicLong version = res.items().isEmpty()
        ? new AtomicLong(0)
        : new AtomicLong(Long.parseLong(res.items().get(0).get(ATTRIBUTE_VERSION).n()));
    long oldStreamVersion = version.intValue();

    if (writeCondition instanceof StreamVersionWriteCondition) {
      org.occurrent.condition.Condition<Long> condition = ((StreamVersionWriteCondition) writeCondition).condition;
      if (condition != null && !LongConditionEvaluator.evaluate(condition, oldStreamVersion)) {
        throw new WriteConditionNotFulfilledException(streamId, oldStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition, version.longValue()));
      }
    }
    List<Pair<String,Long>> eventData = events
        .map(e -> {
          long v = version.incrementAndGet();
          return new Pair<>(OccurrentCloudEventMongoDocumentMapper.convertToDocument(timeRepresentation, streamId, v, e).toJson(), v);
        })
        .collect(Collectors.toList());

    // TODO: optimize
    if (eventData.isEmpty()) {
      return new WriteResult(streamId, oldStreamVersion, oldStreamVersion);
    }

    try {
      dynamoDB.transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(
              eventData.stream().map(pair -> TransactWriteItem.builder().put(Put.builder()
                  .tableName(tableName)
                  .item(Map.of(
                      ATTRIBUTE_STREAM, AttributeValue.builder().s(streamId).build(),
                      ATTRIBUTE_VERSION, AttributeValue.builder().n(Long.toString(pair.t2)).build(),
                      ATTRIBUTE_EVENTS, AttributeValue.builder().s(pair.t1).build()
                  ))
                  .conditionExpression("attribute_not_exists(" + ATTRIBUTE_VERSION + ")")
                  .build()).build()).collect(Collectors.toList())).build());
    } catch (TransactionCanceledException e) {
      // TODO: if WriteCondition.anyStreamVersion maybe we could automatically retry?
      if (e.cancellationReasons().stream().anyMatch(r -> r.code().equals("ConditionalCheckFailed"))) {
        throw new ConcurrentModificationException();
//        throw new WriteConditionNotFulfilledException(streamId, oldStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition, oldStreamVersion));
      } else {
        // transaction failed for some other reason
        throw e;
      }
    }

    return new WriteResult(streamId, oldStreamVersion, version.longValue());
  }

  private static class EventStreamImpl<T> implements EventStream<T> {
    private final String id;
    private final long version;
    private final Stream<T> events;

    EventStreamImpl(String id, long version, Stream<T> events) {
      this.id = id;
      this.version = version;
      this.events = events;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public Stream<T> events() {
      return events;
    }
  }

  @Override
  public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
    QueryResponse res = dynamoDB.query(QueryRequest.builder()
        .tableName(tableName)
        .attributesToGet(ATTRIBUTE_VERSION, ATTRIBUTE_EVENTS)
        .keyConditions(Map.of(
            ATTRIBUTE_STREAM, Condition.builder()
              .attributeValueList(AttributeValue.builder().s(streamId).build())
              .comparisonOperator(ComparisonOperator.EQ)
              .build(),
            ATTRIBUTE_VERSION, Condition.builder()
                .attributeValueList(AttributeValue.builder().n(Long.toString(skip)).build())
                .comparisonOperator(ComparisonOperator.GT)
                .build()
            ))
        .build());
    if (res.items().isEmpty()) {
      return new EventStreamImpl<CloudEvent>(streamId, 0, Collections.<CloudEvent>emptyList().stream());
    }
    long latestVersion = res.items().stream().map(row -> Long.parseLong(row.get(ATTRIBUTE_VERSION).n())).max(Comparator.naturalOrder()).get();
    Stream<CloudEvent> events = res.items().stream()
        .limit(limit)
        .map(row -> row.get(ATTRIBUTE_EVENTS).s())
        .map(Document::parse)
        .map(bson -> OccurrentCloudEventMongoDocumentMapper.convertToCloudEvent(timeRepresentation, bson));
    return new EventStreamImpl<>(streamId, latestVersion, events);
  }

//  @Override
  public void deleteEventStream(String streamId) {
    while (true) {
      QueryResponse queryResponse = dynamoDB.query(QueryRequest.builder()
          .tableName(tableName)
          .attributesToGet(ATTRIBUTE_VERSION)
          .keyConditions(Map.of(
              ATTRIBUTE_STREAM, Condition.builder()
                  .attributeValueList(AttributeValue.builder().s(streamId).build())
                  .comparisonOperator(ComparisonOperator.EQ)
                  .build()
          ))
          .build());
      if (!queryResponse.hasItems() || queryResponse.items().isEmpty()) break;
      BatchWriteItemResponse deleteResponse = dynamoDB.batchWriteItem(
          BatchWriteItemRequest.builder()
              .requestItems(Map.of(tableName,
                  queryResponse.items().stream().map(row -> WriteRequest.builder().deleteRequest(
                      DeleteRequest.builder()
                          .key(Map.of(
                              ATTRIBUTE_STREAM, AttributeValue.builder().s(streamId).build(),
                              ATTRIBUTE_VERSION, row.get(ATTRIBUTE_VERSION)
                          ))
                          .build()).build()).collect(Collectors.toList()))).build());
      if (!deleteResponse.hasUnprocessedItems() && queryResponse.hasLastEvaluatedKey()) break;
      // TODO: add exponential backoff
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean exists(String streamId) {
    return !read(streamId, 0, 1).isEmpty();
  }

}

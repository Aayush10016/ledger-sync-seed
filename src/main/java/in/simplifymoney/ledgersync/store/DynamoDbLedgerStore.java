package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.math.BigDecimal;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DynamoDbLedgerStore implements DocumentStore {

    private final DynamoDbClient client;
    private final String tableName = "LedgerStore";

    public DynamoDbLedgerStore(DynamoDbClient client) {
        this.client = client;
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        try {
            client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
        } catch (ResourceNotFoundException e) {
            client.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.S).build()
                    )
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());
        }
    }

    @Override
    public List<NormalizedTxn> forAccountMonth(String accountLast4, YearMonth month) {
        String pk = "ACCT#" + accountLast4;
        String skPrefix = "TXN#" + month.format(DateTimeFormatter.ofPattern("yyyy-MM"));

        QueryRequest req = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :sk)")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(pk).build(),
                        ":sk", AttributeValue.builder().s(skPrefix).build()
                ))
                .scanIndexForward(false) // Newest first
                .build();

        QueryResponse res = client.query(req);
        System.out.println("forAccountMonth - ScannedCount: " + res.scannedCount() + ", Count: " + res.count());

        List<NormalizedTxn> out = new ArrayList<>();
        for (Map<String, AttributeValue> item : res.items()) {
            out.add(deserialize(item));
        }
        return out;
    }

    @Override
    public Map<Category, BigDecimal> categoryTotals(String accountLast4) {
        String pk = "ACCT#" + accountLast4;
        String skPrefix = "CAT#";

        QueryRequest req = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :sk)")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(pk).build(),
                        ":sk", AttributeValue.builder().s(skPrefix).build()
                ))
                .build();

        QueryResponse res = client.query(req);
        System.out.println("categoryTotals - ScannedCount: " + res.scannedCount() + ", Count: " + res.count());

        Map<Category, BigDecimal> out = new LinkedHashMap<>();
        for (Map<String, AttributeValue> item : res.items()) {
            String catStr = item.get("SK").s().substring(4); // Remove "CAT#"
            BigDecimal total = new BigDecimal(item.get("total_amount").s());
            out.put(Category.valueOf(catStr), total);
        }
        return out;
    }

    @Override
    public Optional<NormalizedTxn> byMessageId(String messageId) {
        String pk = "MSG#" + messageId;
        String sk = "MSG";

        GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(
                        "PK", AttributeValue.builder().s(pk).build(),
                        "SK", AttributeValue.builder().s(sk).build()
                ))
                .build();

        GetItemResponse res = client.getItem(req);
        // getItem inherently examines 1 item and returns 1 item (if exists)
        System.out.println("byMessageId - Item found: " + res.hasItem());

        if (res.hasItem()) {
            return Optional.of(deserialize(res.item()));
        }
        return Optional.empty();
    }

    @Override
    public void save(NormalizedTxn txn) {
        String month = txn.occurredAt().format(DateTimeFormatter.ofPattern("yyyy-MM"));
        String acctPk = "ACCT#" + txn.accountLast4();
        // Deterministic SK based on transaction properties to ensure idempotency across retries
        String hash = String.valueOf(Math.abs(Objects.hash(txn.direction(), txn.amount(), txn.category())));
        String txnSk = "TXN#" + month + "#" + txn.occurredAt().toString() + "#" + hash;

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK", AttributeValue.builder().s(acctPk).build());
        item.put("SK", AttributeValue.builder().s(txnSk).build());
        item.put("accountLast4", AttributeValue.builder().s(txn.accountLast4()).build());
        item.put("occurredAt", AttributeValue.builder().s(txn.occurredAt().toString()).build());
        item.put("direction", AttributeValue.builder().s(txn.direction().name()).build());
        item.put("amount", AttributeValue.builder().s(txn.amount().toPlainString()).build());
        item.put("category", AttributeValue.builder().s(txn.category().name()).build());
        if (txn.merchant() != null) {
            item.put("merchant", AttributeValue.builder().s(txn.merchant()).build());
        }
        if (!txn.sourceMessageIds().isEmpty()) {
            item.put("sourceMessageIds", AttributeValue.builder().ss(txn.sourceMessageIds()).build());
        }

        List<TransactWriteItem> writeItems = new ArrayList<>();
        writeItems.add(TransactWriteItem.builder()
                .put(Put.builder().tableName(tableName).item(item).build())
                .build());

        // Update Category Total
        String catSk = "CAT#" + txn.category().name();
        writeItems.add(TransactWriteItem.builder()
                .update(Update.builder()
                        .tableName(tableName)
                        .key(Map.of(
                                "PK", AttributeValue.builder().s(acctPk).build(),
                                "SK", AttributeValue.builder().s(catSk).build()
                        ))
                        .updateExpression("ADD total_amount :amt")
                        .expressionAttributeValues(Map.of(
                                ":amt", AttributeValue.builder().n(txn.amount().toPlainString()).build()
                        ))
                        .build())
                .build());

        // Message Indices
        for (String msgId : txn.sourceMessageIds()) {
            Map<String, AttributeValue> msgItem = new HashMap<>(item);
            msgItem.put("PK", AttributeValue.builder().s("MSG#" + msgId).build());
            msgItem.put("SK", AttributeValue.builder().s("MSG").build());
            writeItems.add(TransactWriteItem.builder()
                    .put(Put.builder().tableName(tableName).item(msgItem).build())
                    .build());
        }

        // Limit is 100 per TransactWriteItems, but this is at most 1 (Txn) + 1 (Cat) + N (Msgs) which is small.
        client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build());
    }

    private NormalizedTxn deserialize(Map<String, AttributeValue> item) {
        return new NormalizedTxn(
                item.get("accountLast4").s(),
                java.time.OffsetDateTime.parse(item.get("occurredAt").s()),
                in.simplifymoney.ledgersync.model.Direction.valueOf(item.get("direction").s()),
                new BigDecimal(item.get("amount").s()),
                in.simplifymoney.ledgersync.model.Category.valueOf(item.get("category").s()),
                item.containsKey("merchant") ? item.get("merchant").s() : null,
                item.containsKey("sourceMessageIds") ? item.get("sourceMessageIds").ss() : List.of()
        );
    }
}

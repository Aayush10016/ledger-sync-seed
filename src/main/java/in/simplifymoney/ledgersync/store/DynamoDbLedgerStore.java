package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.util.TxnIdentity;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.math.BigDecimal;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
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

        Map<Category, BigDecimal> out = new LinkedHashMap<>();
        for (Map<String, AttributeValue> item : res.items()) {
            String catStr = item.get("SK").s().substring(4); // Remove "CAT#"
            BigDecimal total = new BigDecimal(item.get("total_amount").n());
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

        if (res.hasItem()) {
            Map<String, AttributeValue> item = res.item();
            if (item.containsKey("targetPk") && item.containsKey("targetSk")) {
                GetItemResponse targetRes = client.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("PK", item.get("targetPk"), "SK", item.get("targetSk")))
                        .build());
                if (targetRes.hasItem()) {
                    return Optional.of(deserialize(targetRes.item()));
                }
                return Optional.empty(); // Pointer exists but target is missing
            }
            // Legacy fallback for old records
            return Optional.of(deserialize(item));
        }
        return Optional.empty();
    }

    @Override
    public void save(NormalizedTxn txn) {
        String month = txn.occurredAt().format(DateTimeFormatter.ofPattern("yyyy-MM"));
        String acctPk = "ACCT#" + txn.accountLast4();
        
        // Strict identity: use the canonical ID (first sorted message ID)
        String txnId = TxnIdentity.getId(txn);
        String txnSk = "TXN#" + month + "#" + txn.occurredAt().toEpochSecond() + "#" + txnId;

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
        
        // 1. Put Main Transaction (fail if exists)
        writeItems.add(TransactWriteItem.builder()
                .put(Put.builder()
                        .tableName(tableName)
                        .item(item)
                        .conditionExpression("attribute_not_exists(PK)")
                        .build())
                .build());

        // 2. Update Category Total
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

        // 3. Message Indices
        for (String msgId : txn.sourceMessageIds()) {
            Map<String, AttributeValue> msgItem = new HashMap<>();
            msgItem.put("PK", AttributeValue.builder().s("MSG#" + msgId).build());
            msgItem.put("SK", AttributeValue.builder().s("MSG").build());
            msgItem.put("targetPk", AttributeValue.builder().s(acctPk).build());
            msgItem.put("targetSk", AttributeValue.builder().s(txnSk).build());
            writeItems.add(TransactWriteItem.builder()
                    .put(Put.builder()
                            .tableName(tableName)
                            .item(msgItem)
                            .conditionExpression("attribute_not_exists(PK)")
                            .build())
                    .build());
        }

        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build());
        } catch (TransactionCanceledException e) {
            boolean mainTxnFailed = false;
            boolean messageIndexFailed = false;

            if (e.cancellationReasons() != null && !e.cancellationReasons().isEmpty()) {
                if ("ConditionalCheckFailed".equals(e.cancellationReasons().get(0).code())) {
                    mainTxnFailed = true;
                }
                
                for (int i = 2; i < e.cancellationReasons().size(); i++) {
                    if ("ConditionalCheckFailed".equals(e.cancellationReasons().get(i).code())) {
                        messageIndexFailed = true;
                        break;
                    }
                }
            }

            if (mainTxnFailed) {
                handleExistingTransaction(txn, acctPk, txnSk);
            } else if (messageIndexFailed) {
                throw new IllegalStateException("Transaction failed: A message ID already exists and belongs to a different transaction.", e);
            } else {
                throw new IllegalStateException("Transaction failed due to unknown reasons: " + e.getMessage(), e);
            }
        }
    }

    private void handleExistingTransaction(NormalizedTxn txn, String acctPk, String txnSk) {
        // The transaction already exists. We only need to add any NEW message IDs to it.
        // We use TransactWriteItems to atomically update the sourceMessageIds and add new MSG# indices.
        // We DO NOT update category totals.
        
        List<TransactWriteItem> writeItems = new ArrayList<>();
        
        // Add new message IDs to the existing transaction
        writeItems.add(TransactWriteItem.builder()
                .update(Update.builder()
                        .tableName(tableName)
                        .key(Map.of("PK", AttributeValue.builder().s(acctPk).build(), "SK", AttributeValue.builder().s(txnSk).build()))
                        .updateExpression("ADD sourceMessageIds :newIds")
                        .expressionAttributeValues(Map.of(":newIds", AttributeValue.builder().ss(txn.sourceMessageIds()).build()))
                        .build())
                .build());

        // Attempt to create MSG# indices for ALL message IDs (only if they don't exist yet)
        for (String msgId : txn.sourceMessageIds()) {
            Map<String, AttributeValue> msgItem = new HashMap<>();
            msgItem.put("PK", AttributeValue.builder().s("MSG#" + msgId).build());
            msgItem.put("SK", AttributeValue.builder().s("MSG").build());
            msgItem.put("targetPk", AttributeValue.builder().s(acctPk).build());
            msgItem.put("targetSk", AttributeValue.builder().s(txnSk).build());
            writeItems.add(TransactWriteItem.builder()
                    .put(Put.builder()
                            .tableName(tableName)
                            .item(msgItem)
                            // We use a conditional put. If the MSG# already exists, we will catch the failure below.
                            .conditionExpression("attribute_not_exists(PK)")
                            .build())
                    .build());
        }

        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build());
        } catch (TransactionCanceledException e) {
            // It's perfectly fine if some MSG# indices already exist (they belong to this very transaction!)
            // However, TransactWriteItems will fail the entire transaction if ANY condition fails.
            // If we hit a ConditionalCheckFailed on a MSG# index, it means the index already exists.
            // Let's verify that the existing MSG# index actually points to THIS transaction.
            
            for (String msgId : txn.sourceMessageIds()) {
                Optional<NormalizedTxn> existingTarget = byMessageId(msgId);
                if (existingTarget.isPresent()) {
                    String existingId = TxnIdentity.getId(existingTarget.get());
                    String currentId = TxnIdentity.getId(txn);
                    if (!existingId.equals(currentId)) {
                        throw new IllegalStateException("Conflict: Message ID " + msgId + " belongs to a different transaction!");
                    }
                }
            }
            
            // If all existing MSG# indices point to this transaction, then there is nothing left to do!
            // The ADD sourceMessageIds was either already done or isn't needed.
            // Wait, what if we needed to add a NEW message ID but an OLD message ID failed the conditional check?
            // TransactWriteItems fails entirely. We must retry the update, filtering out existing MSG# indices.
            retryPartialUpdate(txn, acctPk, txnSk);
        }
    }
    
    private void retryPartialUpdate(NormalizedTxn txn, String acctPk, String txnSk) {
        List<TransactWriteItem> writeItems = new ArrayList<>();
        List<String> newIdsToAdd = new ArrayList<>();
        
        for (String msgId : txn.sourceMessageIds()) {
            Map<String, AttributeValue> key = Map.of(
                "PK", AttributeValue.builder().s("MSG#" + msgId).build(),
                "SK", AttributeValue.builder().s("MSG").build()
            );
            
            GetItemResponse res = client.getItem(GetItemRequest.builder().tableName(tableName).key(key).build());
            if (!res.hasItem()) {
                newIdsToAdd.add(msgId);
                
                Map<String, AttributeValue> msgItem = new HashMap<>(key);
                msgItem.put("targetPk", AttributeValue.builder().s(acctPk).build());
                msgItem.put("targetSk", AttributeValue.builder().s(txnSk).build());
                
                writeItems.add(TransactWriteItem.builder()
                        .put(Put.builder()
                                .tableName(tableName)
                                .item(msgItem)
                                .conditionExpression("attribute_not_exists(PK)")
                                .build())
                        .build());
            }
        }
        
        if (!newIdsToAdd.isEmpty()) {
            writeItems.add(TransactWriteItem.builder()
                    .update(Update.builder()
                            .tableName(tableName)
                            .key(Map.of("PK", AttributeValue.builder().s(acctPk).build(), "SK", AttributeValue.builder().s(txnSk).build()))
                            .updateExpression("ADD sourceMessageIds :newIds")
                            .expressionAttributeValues(Map.of(":newIds", AttributeValue.builder().ss(newIdsToAdd).build()))
                            .build())
                    .build());
                    
            try {
                client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build());
            } catch (TransactionCanceledException e) {
                throw new IllegalStateException("Failed to update transaction with new message IDs due to concurrent modification", e);
            }
        }
    }

    private NormalizedTxn deserialize(Map<String, AttributeValue> item) {
        List<String> sortedIds = item.containsKey("sourceMessageIds") 
            ? new ArrayList<>(item.get("sourceMessageIds").ss())
            : new ArrayList<>();
        Collections.sort(sortedIds);

        return new NormalizedTxn(
                item.get("accountLast4").s(),
                java.time.OffsetDateTime.parse(item.get("occurredAt").s()),
                in.simplifymoney.ledgersync.model.Direction.valueOf(item.get("direction").s()),
                new BigDecimal(item.get("amount").s()),
                in.simplifymoney.ledgersync.model.Category.valueOf(item.get("category").s()),
                item.containsKey("merchant") ? item.get("merchant").s() : null,
                sortedIds
        );
    }
}

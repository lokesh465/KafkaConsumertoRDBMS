ki package Kafka.Work.Consumer.main;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
}Test
    void testCreateOptimizedPayload_successfulFlow() throws Exception {
        String inputJson = """
        {
            "contractId": 123,
            "hashValue": "abc123",
            "eventName": "CONTRACT_CREATED",
            "productType": "CASH",
            "events": {
                "eventPayload": { "field": "value" }
            },
            "swapContract": [ { "key": "val" } ],
            "cashContract": [ { "key": "val" } ]
        }
        """;
        var consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, inputJson);
        var headers = new RecordHeaders();
        consumerRecord.headers().add("eventType", "SWAP".getBytes(StandardCharsets.UTF_8));

        when(findElementUtil.extractValuesOutOfPayload(any(), eq("contractId")))
            .thenReturn(Optional.of(123));
        when(findElementUtil.extractValuesOutOfPayload(any(), eq("eventName")))
            .thenReturn(Optional.of("CONTRACT_CREATED"));
        when(findElementUtil.extractValuesOutOfPayload(any(), eq("productType")))
            .thenReturn(Optional.of("CASH"));
        when(findElementUtil.extractValuesOutOfPayload(any(), eq("hashValue")))
            .thenReturn(Optional.of("abc123"));

        when(contractKafkaProducer.kafkaWrite(any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Map.of("ctx", "val")));
        when(outAcknowledgement.createAcknowledgementPayload(any()))
            .thenReturn(mock(JsonNode.class));
        when(payloadRefactorService.payloadRefactor(anyString()))
            .thenReturn(mock(JsonNode.class));

        service.createOptimizedPayload("txid-123", consumerRecord);

        verify(contractMongoService, times(1)).persistData(any());
        verify(contractKafkaProducer, atLeastOnce()).kafkaWrite(any(), any(), any(), anyMap(), any());
        verify(outAcknowledgement).createAcknowledgementPayload(any());
    }

    @Test
    void testCreateOptimizedPayload_contractCancelledFlow() throws Exception {
        String inputJson = """
        {
            "contractId": 123,
            "hashValue": "abc123",
            "eventName": "CONTRACT_CANCELLED",
            "productType": "SWAP",
            "events": {
                "eventPayload": { "field": "value" }
            }
        }
        """;
        var consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, inputJson);

        when(findElementUtil.extractValuesOutOfPayload(any(), eq("contractId")))
            .thenReturn(Optional.of(123));
        when(findElementUtil.extractValuesOutOfPayload(any(), eq("eventName")))
            .thenReturn(Optional.of("CONTRACT_CANCELLED"));
        when(findElementUtil.extractValuesOutOfPayload(any(), eq("productType")))
            .thenReturn(Optional.of("SWAP"));
        when(findElementUtil.extractValuesOutOfPayload(any(), eq("hashValue")))
            .thenReturn(Optional.of("abc123"));

        when(contractKafkaProducer.kafkaWrite(any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(Map.of("ctx", "val")));
        when(outAcknowledgement.createAcknowledgementPayload(any()))
            .thenReturn(mock(JsonNode.class));
        when(payloadRefactorService.payloadRefactor(anyString()))
            .thenReturn(mock(JsonNode.class));

        service.createOptimizedPayload("txid-456", consumerRecord);

        verify(contractMongoService).processContractDelete(any(), eq(123), eq("CONTRACT_CANCELLED"));
    }

    @Test
    void testCreateOptimizedPayload_missingContractId_shouldThrow() {
        var consumerRecord = new ConsumerRecord<>("topic", 0, 0L, null, "{}");

        when(findElementUtil.extractValuesOutOfPayload(any(), eq("contractId")))
            .thenReturn(Optional.empty());

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> service.createOptimizedPayload("txid-missing", consumerRecord));
    }



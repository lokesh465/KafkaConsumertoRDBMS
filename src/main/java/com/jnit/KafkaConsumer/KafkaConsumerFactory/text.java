import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public CompletableFuture<String> kafkaWrite(String inputJson, String contractProducerType) {
    KafkaRegister kafkaRegister = new KafkaRegister(applicationContext);
    KafkaTemplate<String, String> template = kafkaRegister.getProducerKafkaTemplate(contractProducerType);

    // Sending the message to Kafka and obtaining ListenableFuture
    ListenableFuture<SendResult<String, String>> listenableFuture = template.send(kafkaRegister.getProducerTopic(contractProducerType), UUID.randomUUID().toString(), inputJson);

    // Create a CompletableFuture
    CompletableFuture<String> completableFuture = new CompletableFuture<>();

    // Add a callback to the ListenableFuture
    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
        @Override
        public void onSuccess(SendResult<String, String> result) {
            // Completing the CompletableFuture with success
            completableFuture.complete("Success");
        }

        @Override
        public void onFailure(Throwable ex) {
            // Completing the CompletableFuture exceptionally in case of failure
            completableFuture.completeExceptionally(ex);
        }
    });

    // Return the CompletableFuture to be handled asynchronously
    return completableFuture;
}
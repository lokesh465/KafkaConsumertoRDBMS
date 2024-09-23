
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public CompletableFuture<String> kafkaWrite(String inputJson, String contractProducerType) {
    KafkaRegister kafkaRegister = new KafkaRegister(applicationContext);
    KafkaTemplate<String, String> template = kafkaRegister.getProducerKafkaTemplate(contractProducerType);

    // Sending the message to Kafka
    ListenableFuture<SendResult<String, String>> future = template.send(kafkaRegister.getProducerTopic(contractProducerType), UUID.randomUUID().toString(), inputJson);

    // Converting ListenableFuture to CompletableFuture
    CompletableFuture<String> completableFuture = new CompletableFuture<>();

    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
        @Override
        public void onSuccess(SendResult<String, String> result) {
            completableFuture.complete("Success");
        }

        @Override
        public void onFailure(Throwable ex) {
            completableFuture.completeExceptionally(ex);
        }
    });

    return completableFuture;
}
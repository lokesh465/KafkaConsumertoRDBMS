public CompletableFuture<String> kafkaWrite(String inputJson, String contractProducerType) {
    KafkaRegister kafkaRegister = new KafkaRegister(applicationContext);
    KafkaTemplate<String, String> template = kafkaRegister.getProducerKafkaTemplate(contractProducerType);

    return template.send(kafkaRegister.getProducerTopic(contractProducerType), UUID.randomUUID().toString(), inputJson)
        .completable()
        .thenApply(result -> {
            // Handle success scenario
            return "Success";
        })
        .exceptionally(ex -> {
            // Handle error scenario
            return "Failure: " + ex.getMessage();
        });
}
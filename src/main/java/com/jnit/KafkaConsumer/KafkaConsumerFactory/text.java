public Mono<Object> fluxParser(String kafkaInContractPayload, String contractIdentifier) {
    Logger.info("Started comparing Kafka record with Mongo record for Contract Identifier: {}", contractIdentifier);

    // Create the query and sort by timestamp (or any other field to get the latest document)
    var query = new Query();
    query.addCriteria(Criteria.where(PayloadConstants.Contract_Identifier).is(contractIdentifier))
         .with(Sort.by(Sort.Direction.DESC, "timestamp"))  // Assuming "timestamp" is your sorting field
         .limit(1);

    // Fetch the latest document from MongoDB
    return Mono.justOrEmpty(mongoTemplate.findOne(query, Document.class, mongoCollection))
            .flatMap(doc -> {
                Logger.info("Retrieved latest Document from Mongo based on Contract Identifier: {}", contractIdentifier);
                
                var mongoContractPayload = new JSONObject(doc.toJson());
                
                // Identify contract type and extract nested JSON
                if (mongoContractPayload.has(PayloadConstants.CASH_CONTRACT)) {
                    mongoContractPayload = mongoContractPayload.getJSONObject(PayloadConstants.CASH_CONTRACT);
                } else if (mongoContractPayload.has(PayloadConstants.SWAP_CONTRACT)) {
                    mongoContractPayload = mongoContractPayload.getJSONObject(PayloadConstants.SWAP_CONTRACT);
                }

                var objectMapper = new ObjectMapper();
                var kafkaMap = objectMapper.readValue(kafkaInContractPayload, Map.class);

                // Recursively update only matching keys
                return Mono.fromCallable(() -> {
                    var mongoMap = objectMapper.readValue(mongoContractPayload.toString(), Map.class);
                    updateMatchingKeys(mongoMap, kafkaMap);
                    return (Object) mongoMap;
                });
            })
            .switchIfEmpty(Mono.defer(() -> {
                Logger.warn("No document found for Contract Identifier: {}", contractIdentifier);

                // Convert KafkaInPayload to Map and return as fallback
                return Mono.fromCallable(() -> {
                    var objectMapper = new ObjectMapper();
                    return (Object) objectMapper.readValue(kafkaInContractPayload, Map.class);
                });
            }));
}

// Recursively update only existing keys in MongoMap with values from KafkaMap
private void updateMatchingKeys(Map<String, Object> mongoMap, Map<String, Object> kafkaMap) {
    kafkaMap.forEach((key, value) -> {
        if (mongoMap.containsKey(key)) {
            if (value instanceof Map && mongoMap.get(key) instanceof Map) {
                // Recursively merge nested maps
                updateMatchingKeys((Map<String, Object>) mongoMap.get(key), (Map<String, Object>) value);
            } else {
                // Update the value if it's a matching key
                mongoMap.put(key, value);
            }
        }
    });
}

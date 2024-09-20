public Mono<Object> fluxParser(String kafkaInContractPayload, String contractIdentifier) {
    Logger.info("Started comparing Kafka record with Mongo record for Contract Identifier: {}", contractIdentifier);

    // Create the query
    var query = new Query();
    query.addCriteria(Criteria.where(PayloadConstants.Contract_Identifier).is(contractIdentifier));

    // Fetch document from MongoDB
    return Mono.justOrEmpty(mongoTemplate.findOne(query, Document.class, mongoCollection))
            .flatMap(doc -> {
                Logger.info("Retrieved Document from Mongo based on Contract Identifier: {}", contractIdentifier);
                
                var mongoContractPayload = new JSONObject(doc.toJson());

                // Check for contract type
                if (mongoContractPayload.has(PayloadConstants.CASH_CONTRACT)) {
                    mongoContractPayload = mongoContractPayload.getJSONObject(PayloadConstants.CASH_CONTRACT);
                } else if (mongoContractPayload.has(PayloadConstants.SWAP_CONTRACT)) {
                    mongoContractPayload = mongoContractPayload.getJSONObject(PayloadConstants.SWAP_CONTRACT);
                }

                var objectMapper = new ObjectMapper();
                var mongoContractPayloadStr = mongoContractPayload.toString();

                // Parse and merge Kafka and Mongo payloads
                return Mono.fromCallable(() -> {
                    var mongoMap = objectMapper.readValue(mongoContractPayloadStr, Map.class);
                    var kafkaMap = objectMapper.readValue(kafkaInContractPayload, Map.class);

                    // Replace or update the values for matching keys
                    kafkaMap.forEach((key, value) -> {
                        if (mongoMap.containsKey(key)) {
                            // Update the value in Mongo map with the value from Kafka
                            mongoMap.put(key, value);
                        }
                    });

                    return (Object) mongoMap;  // Casting to Object
                });
            })
            .switchIfEmpty(Mono.defer(() -> {
                Logger.warn("No document found for Contract Identifier: {}", contractIdentifier);

                // Convert kafkaInContractPayload to Map and return as fallback
                return Mono.fromCallable(() -> {
                    var objectMapper = new ObjectMapper();
                    return (Object) objectMapper.readValue(kafkaInContractPayload, Map.class);  // Casting to Object
                });
            }));
}
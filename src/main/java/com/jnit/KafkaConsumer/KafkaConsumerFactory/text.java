@Value("${mongoDB.collection}")
private String mongoCollection;

@Value("${mongoDB.field}")
private String mongoField;

@Value("${mongoDB.database}")
private String database;

@Value("${optimized.payload.attributes}")
private String[] attributes;

@Value("${optimized.payload.attributes.array}")
private String[] attributesArray;

public void createOptimizedPayload(String contractPayloadReceived) throws Exception {

    LOGGER.info("Started creating Optimized Payload");
    JSONObject contractPayload = new JSONObject(contractPayloadReceived);

    String contractIdentifier = String.valueOf(extractValuesOutOfPayload(contractPayload, "contractIdentifier").orElse(""));

    MdcInfo mdcInfo = new MdcInfo();

    LOGGER.info("Started parsing into JSONObject to create optimized Payload");
    JSONObject optimizedJSON = new JSONObject();
    optimizedJSON.put("lastupdatedDateTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSZ").format(new Date()));

    processContract(contractPayload, PayloadConstants.SWAP_CONTRACT, optimizedJSON, mdcInfo, contractIdentifier);
    processContract(contractPayload, PayloadConstants.CASH_CONTRACT, optimizedJSON, mdcInfo, contractIdentifier);
    processContract(contractPayload, PayloadConstants.LIVE_TRADE_CONTRACT, optimizedJSON, mdcInfo, contractIdentifier);

    addAttributesToOptimizedPayload(contractPayload, optimizedJSON, attributes);
    addAttributesArrayToOptimizedPayload(contractPayload, optimizedJSON, attributesArray);

    LOGGER.info("################# Optimized JSON: " + optimizedJSON);

    persistData(optimizedJSON);
    sendDataToS3(optimizedJSON);

    LOGGER.info("Completed creating Optimized payload and sent to Mongo, S3 and Kafka out topic");
}

private void processContract(JSONObject contractPayload, String contractType, JSONObject optimizedJSON, MdcInfo mdcInfo, String contractIdentifier) {
    extractValuesOutOfPayload(contractPayload, contractType).ifPresent(contract -> {
        if (contract instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) contract;
            if (jsonArray.length() > 0 && jsonArray.get(0) instanceof JSONObject) {
                contract = jsonArray.getJSONObject(0);
                mdcInfo.setEventType(contractType);
                mdcInfo.setContractIdentifier(contractIdentifier);
            }
        }
        optimizedJSON.put(contractType, contract);
    });
}

private void addAttributesToOptimizedPayload(JSONObject contractPayload, JSONObject optimizedJSON, String[] attributes) {
    for (String elementKey : attributes) {
        Object elementValue = extractValuesOutOfPayload(contractPayload, elementKey).orElse(JSONObject.NULL);
        optimizedJSON.put(elementKey, elementValue);
    }
}

private void addAttributesArrayToOptimizedPayload(JSONObject contractPayload, JSONObject optimizedJSON, String[] attributesArray) {
    for (String elementKey : attributesArray) {
        Set<Object> elementValueSet = extractValuesArrayOutOfPayload(contractPayload, elementKey);
        JSONArray elementValueArray = new JSONArray(elementValueSet);
        optimizedJSON.put(elementKey, elementValueArray.isEmpty() ? JSONObject.NULL : elementValueArray);
    }
}

private void persistData(JSONObject optimizedJSON) throws JsonPersistenceException {
    try {
        persisJsonToMongoDB.processData(optimizedJSON.toString(), mongoCollection, mongoField);
        LOGGER.info("----Contract data is persisted into DB");
    } catch (Exception e) {
        LOGGER.error("Exception while persisting JSON: " + e.getMessage());
        throw new JsonPersistenceException("Exception while persisting JSON", e);
    }
}

private void sendDataToS3(JSONObject optimizedJSON) throws JsonPersistenceException {
    try {
        LOGGER.info("-----------> Started persisting JSON to S3");
        deltaService.insertRecordsWithDate(optimizedJSON.toString(), deltaPath);
        LOGGER.info("------------> Contract data is persisted into S3");
    } catch (Exception e) {
        LOGGER.error("Exception while sending JSON to S3: " + e.getMessage());
        throw new JsonPersistenceException("Exception while sending JSON to S3", e);
    }
}

private Optional<Object> extractValuesOutOfPayload(JSONObject contractPayload, String elementKey) {
    if (contractPayload.has(elementKey)) {
        return Optional.of(contractPayload.get(elementKey));
    }
    
    return contractPayload.keySet().stream()
        .map(k -> contractPayload.get(k))
        .flatMap(value -> {
            if (value instanceof JSONObject) {
                return extractValuesOutOfPayload((JSONObject) value, elementKey).stream();
            } else if (value instanceof JSONArray) {
                return IntStream.range(0, ((JSONArray) value).length())
                    .mapToObj(((JSONArray) value)::get)
                    .filter(JSONObject.class::isInstance)
                    .map(JSONObject.class::cast)
                    .map(nestedObject -> extractValuesOutOfPayload(nestedObject, elementKey))
                    .flatMap(Optional::stream);
            }
            return Stream.empty();
        })
        .findFirst();
}

private Set<Object> extractValuesArrayOutOfPayload(Object sourceJson, String targetKey) {
    Set<Object> resultSet = new HashSet<>();

    if (sourceJson instanceof JSONObject) {
        JSONObject jsonObject = (JSONObject) sourceJson;
        jsonObject.keySet().forEach(key -> {
            Object value = jsonObject.opt(key);
            if (key.equals(targetKey) && (value instanceof String || value instanceof Integer)) {
                resultSet.add(value);
            } else {
                resultSet.addAll(extractValuesArrayOutOfPayload(value, targetKey));
            }
        });
    } else if (sourceJson instanceof JSONArray) {
        JSONArray jsonArray = (JSONArray) sourceJson;
        IntStream.range(0, jsonArray.length())
            .mapToObj(jsonArray::get)
            .forEach(element -> resultSet.addAll(extractValuesArrayOutOfPayload(element, targetKey)));
    }

    return resultSet;
}

private final Logger log = LogManager.getLogger();
DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

// Get JMeter variables
String bootstrapServer = vars.get("bootstrap_servers");
String inputTopic = vars.get("input_topic");
String securityProtocol = vars.get("security_protocol");
String saslMechanism = vars.get("sasl_mechanism");
String producerUsername = vars.get("producer_username");
String producerPassword = vars.get("producer_password");
String jsonPayload = vars.get("json_payload");

String mongoUsername = vars.get("mongo_username");
String mongoPassword = vars.get("mongo_password");
String mongodbHost = vars.get("mongodb_host");
String mongodbName = vars.get("mongodb_name");
String mongodbOptions = vars.get("mongodb_options");

// Initialize counters for success and failure
int successCounter = 0;
int failureCounter = 0;

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
props.put(SaslConfigs.SASL_JAAS_CONFIG, PlainLoginModule.class.getName() + " required username=\"" + producerUsername + "\" password=\"" + producerPassword + "\";");

KafkaProducer<String, String> producer = null;
String contractIdentifierStr = null;
long totalDuration = 0L;

try (MongoClient mongoClient = MongoClients.create("mongodb+srv://" + mongoUsername + ":" + mongoPassword + "@" + mongodbHost + "/" + mongodbName + "?" + mongodbOptions)) {

    log.info("-------MongoDB Connection Established-----------");

    // Parse and modify JSON payload
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(jsonPayload);

    JsonNode contractIdentifierNode = jsonNode.findPath("contractIdentifier");
    if (!contractIdentifierNode.isMissingNode()) {
        contractIdentifierStr = contractIdentifierNode.asText().trim();
        long contractIdLong = Long.parseLong(contractIdentifierStr);

        int numRecords = 10;
        producer = new KafkaProducer<>(props);

        for (int i = 0; i < numRecords; i++) {
            contractIdentifierStr = String.valueOf(contractIdLong + i);
            ((ObjectNode) jsonNode).put("contractIdentifier", contractIdentifierStr);
            String updatedJson = mapper.writeValueAsString(jsonNode);

            // Send message to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(inputTopic, null, updatedJson);
            producer.send(record);

            // Track start time
            String startTime = OffsetDateTime.now().format(dateTimeFormat);

            // Query MongoDB
            Document doc = mongoClient.getDatabase(mongodbName).getCollection("contract")
                .find(Filters.eq("contractIdentifier", contractIdentifierStr)).first();

            if (doc != null) {
                JSONObject json = new JSONObject(doc.toJson());
                String createdTimestamp = json.optString("lastUpdatedDateTime");
                Duration difference = Duration.between(OffsetDateTime.parse(startTime), OffsetDateTime.parse(createdTimestamp));

                totalDuration += difference.toMillis();
                successCounter++;  // Increment success counter

                log.info("Record {} persisted in {} ms", contractIdentifierStr, difference.toMillis());
                SampleResult.setSuccessful(true);
                SampleResult.setResponseMessage("Record persisted in DB. Time taken: " + difference.toMillis() + " ms");
                SampleResult.setResponseData("Time taken to persist record: " + difference.toMillis() + " ms", "UTF-8");

            } else {
                log.warn("No document found for contractIdentifier {}", contractIdentifierStr);
                failureCounter++;  // Increment failure counter

                SampleResult.setSuccessful(false);
                SampleResult.setResponseMessage("No document found for contractIdentifier " + contractIdentifierStr);
            }

            // Add delay between records if necessary
            Thread.sleep(3000);
        }

        // Set final results for the test
        SampleResult.setSuccessful(true);
        SampleResult.setResponseMessage("Total time taken for all records: " + totalDuration + " ms");
        SampleResult.setResponseData("Total time for persistence: " + totalDuration + " ms", "UTF-8");

    } else {
        log.error("Contract Identifier not found in JSON payload");
        SampleResult.setSuccessful(false);
        SampleResult.setResponseMessage("Contract Identifier not found in JSON payload");
    }

} catch (Exception e) {
    log.error("Error during Kafka-MongoDB operation", e);
    SampleResult.setSuccessful(false);
    SampleResult.setResponseMessage("Error during test execution: " + e.getMessage());
    SampleResult.setResponseData("Error details: " + e.toString(), "UTF-8");
} finally {
    if (producer != null) {
        producer.close();
    }

    // Log the final success and failure counts
    log.info("Success Count: {}", successCounter);
    log.info("Failure Count: {}", failureCounter);

    // Add success and failure counts to the sample result
    SampleResult.sampleStart();
    SampleResult.setResponseMessage("Success Count: " + successCounter + ", Failure Count: " + failureCounter);
    SampleResult.setResponseData("Success Count: " + successCounter + ", Failure Count: " + failureCounter, "UTF-8");

    // Send metrics for visualization
    vars.put("SuccessCounter", String.valueOf(successCounter));
    vars.put("FailureCounter", String.valueOf(failureCounter));

    log.info("################# Test Finished #####################");
}

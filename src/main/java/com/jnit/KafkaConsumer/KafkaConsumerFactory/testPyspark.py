import os
import json
from datetime import datetime
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Optimized Payload Creation") \
    .getOrCreate()

# Mock configuration for attributes
attributes = ["attribute1", "attribute2", "attribute3"]  # Replace with actual attributes
attributes_array = ["arrayAttribute1", "arrayAttribute2"]  # Replace with actual attributes

def extract_values_out_of_payload(payload, key):
    """Recursive function to extract values by key from nested JSON."""
    if isinstance(payload, dict):
        if key in payload:
            return payload[key]
        for k, v in payload.items():
            result = extract_values_out_of_payload(v, key)
            if result is not None:
                return result
    elif isinstance(payload, list):
        for item in payload:
            result = extract_values_out_of_payload(item, key)
            if result is not None:
                return result
    return None

def extract_values_array_out_of_payload(payload, key):
    """Recursive function to extract values as a set from nested JSON."""
    result_set = set()
    if isinstance(payload, dict):
        for k, v in payload.items():
            if k == key and isinstance(v, (str, int)):
                result_set.add(v)
            else:
                result_set.update(extract_values_array_out_of_payload(v, key))
    elif isinstance(payload, list):
        for item in payload:
            result_set.update(extract_values_array_out_of_payload(item, key))
    return result_set

def create_optimized_payload(contract_payload):
    """Main function to create the optimized payload."""
    optimized_json = {
        "lastUpdatedDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S:%fZ")
    }

    contract_identifier = extract_values_out_of_payload(contract_payload, "contractIdentifier") or ""

    process_contract(contract_payload, "SWAP_CONTRACT", optimized_json, contract_identifier)
    process_contract(contract_payload, "CASH_CONTRACT", optimized_json, contract_identifier)
    process_contract(contract_payload, "LIVE_TRADE_CONTRACT", optimized_json, contract_identifier)

    add_attributes_to_optimized_payload(contract_payload, optimized_json, attributes)
    add_attributes_array_to_optimized_payload(contract_payload, optimized_json, attributes_array)

    print("Optimized JSON:", json.dumps(optimized_json, indent=2))
    return optimized_json

def process_contract(contract_payload, contract_type, optimized_json, contract_identifier):
    """Processes a contract type and updates the optimized payload."""
    contract = extract_values_out_of_payload(contract_payload, contract_type)
    if isinstance(contract, list) and len(contract) > 0 and isinstance(contract[0], dict):
        contract = contract[0]
    optimized_json[contract_type] = contract

def add_attributes_to_optimized_payload(contract_payload, optimized_json, attributes):
    """Adds single-value attributes to the optimized payload."""
    for key in attributes:
        value = extract_values_out_of_payload(contract_payload, key)
        optimized_json[key] = value if value is not None else None

def add_attributes_array_to_optimized_payload(contract_payload, optimized_json, attributes_array):
    """Adds array attributes to the optimized payload."""
    for key in attributes_array:
        value_set = extract_values_array_out_of_payload(contract_payload, key)
        optimized_json[key] = list(value_set) if value_set else None

# Main entry point
if __name__ == "__main__":
    # Determine the path to the resources folder
    resources_path = os.path.join(os.path.dirname(__file__), "resources", "contractPayload.json")

    # Read the JSON payload from the resources folder
    with open(resources_path, "r") as file:
        contract_payload_received = json.load(file)

    # Create the optimized payload
    optimized_payload = create_optimized_payload(contract_payload_received)

    # Save or process the optimized payload as needed

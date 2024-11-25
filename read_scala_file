import os
import re
import pandas as pd

# Input file path
input_file = "/example/data/job.scala"

# Create a list to hold tuples of (file_name, object_name, param1, <param2>, <param3>, <param4>)
stream_data = []

# Function to extract param1, param2, param3, and param4 from the content of an object/trait
def extract_details(entity_name, content):
    # Updated pattern for param1, excluding def
    param1_pattern = r'\bparam1\s*=\s*"(.*?)"'

    # Find param1 within the object/trait
    param1_match = re.search(param1_pattern, content)
    param1 = param1_match.group(1) if param1_match else ""

    # Find the param2, param3, and param4 override from the environment block
    param2_pattern = r'"<input_param_pattern_in_file>"\s*->\s*"(.*?)"'
    param3_pattern = r'"<param2_pattern>"\s*->\s*"(.*?)"'
    param4_pattern = r'"<param4_pattern>"\s*->\s*"(.*?)"'

    param2_match = re.search(param2_pattern, content)
    param3_match = re.search(param3_pattern, content)
    param4_match = re.search(param4_pattern, content)

    <param2> = param2_match.group(1) if param2_match else ""
    <param3>_override = param3_match.group(1) if param3_match else ""
    <param4> = param4_match.group(1) if param4_match else ""

    return (param1, <param2>, <param>_override, <param4>)

# Read the Scala file
with open(input_file, 'r', encoding='utf-8') as file:
    content = file.read()

    # Use regex to find all occurrences of objects and traits
    object_pattern = r'(object|trait)\s+(\w+)\s*'
    entities = re.findall(object_pattern, content)

    # Process each entity and extract its details
    for entity_type, entity_name in entities:
        # Extract the content specific to the current object or trait
        entity_content_pattern = r'(object|trait)\s+' + re.escape(entity_name) + r'(.*?)\n(?:object|trait|\Z)'
        entity_content_match = re.search(entity_content_pattern, content, re.DOTALL)

        if entity_content_match:
            entity_content = entity_content_match.group(2)

            # Extract the param1, param2, <param3>_override, and param4 override
            param1, <param2>, param2, <param4> = extract_details(entity_name, entity_content)

            # Append the details to the stream_data list
            stream_data.append((entity_name, param1, <param2>, input_path_override, <param4>))

# Create a DataFrame and save to a TSV file
df = pd.DataFrame(stream_data, columns=['Object/Trait Name', 'param1', 'param2', 'param3 Override', 'param4 Override'])
output_file = '/example/data/entity_data_with_param1_v2.tsv'
df.to_csv(output_file, sep='\t', index=False)

print(f"Entity data with param1 written to {output_file}")

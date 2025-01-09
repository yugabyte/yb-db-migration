#!/usr/bin/env python3

import yaml

def generate_yaml(num_tables=1250):
    config = {
        "file_table_map": "",
        "additional_flags": {
            "--delimiter": ",",
            "--format": "csv",
            "--has-header": "true"
        },
        "row_count": {},
        "resumption": {
            "max_restarts": 50,
            "min_interrupt_seconds": 15,
            "max_interrupt_seconds": 30,
            "min_restart_wait_seconds": 15,
            "max_restart_wait_seconds": 30
        }
    }

    # Generate table mappings
    table_mappings = []
    for i in range(1, num_tables + 1):
        table_name = f"survey{i}"
        table_mappings.append(f"FY2021_Survey.csv:{table_name}")
        config["row_count"][table_name] = 41715  # Default row count for each table

    config["file_table_map"] = ",".join(table_mappings)

    # Generate YAML content
    yaml_content = yaml.dump(config, default_flow_style=False)

    with open("config.yaml", "w") as yaml_file:
        yaml_file.write(yaml_content)

    print("YAML file generated successfully.")

# Generate for 1250 tables
generate_yaml(1250)

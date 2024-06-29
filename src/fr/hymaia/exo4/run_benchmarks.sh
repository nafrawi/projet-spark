#!/bin/bash

# Define the output file path
OUTPUT_FILE="/home/nafra/Bureau/code/spark-handson/results/benchmarks.txt"

# Clear the output file or create if not exist
> "$OUTPUT_FILE"

# Directory where scripts are located
SCRIPT_DIR="/home/nafra/Bureau/code/spark-handson"

echo "Script started. Output file: $OUTPUT_FILE" >&2

# Function to run a script and log its output
run_and_log() {
    local script_name="$1"

    echo "Attempting to change directory to $SCRIPT_DIR" >&2
    cd "$SCRIPT_DIR" || { echo "Failed to change directory to $SCRIPT_DIR" >&2; exit 1; }
    echo "Running $script_name in $(pwd)" >&2

    echo "Executing: poetry run $script_name" >&2
    local results=$(poetry run "$script_name" 2>&1)

    if [ -z "$results" ]; then
        echo "No output was captured from $script_name." >&2
    else
        echo "Output was captured from $script_name." >&2
        echo "First few lines of output:" >&2
        echo "$results" | head -n 5 >&2
    fi

    echo "Logging results to $OUTPUT_FILE" >&2
    {
        echo "--------------------------------------------------"
        echo "File Name: $script_name"
        echo "Date: $(date)"
        echo "Results:"
        echo "$results"
        echo "--------------------------------------------------"
    } >> "$OUTPUT_FILE"

    echo "Finished logging $script_name" >&2
}

# Test write before running scripts
echo "Test write before runs" >> "$OUTPUT_FILE"

# Run and log each script
run_and_log "no_udf"
run_and_log "scala_udf"
run_and_log "python_udf"
run_and_log "pandas_udf"

poetry run plot 

echo "Test write after runs" >> "$OUTPUT_FILE"

echo "Benchmarks completed. " >&2

cat "$

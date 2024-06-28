#!/bin/bash

OUTPUT_FILE="benchmarks.txt"
SCRIPT_DIR="/home/nafra/Bureau/code/spark-handson"

> "$OUTPUT_FILE"

echo "Script started. Output file: $OUTPUT_FILE" >&2

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

echo "Test write before runs" >> "$OUTPUT_FILE"
run_and_log "no_udf"
run_and_log "scala_udf"
run_and_log "python_udf"
run_and_log "pandas_udf"
poetry run plot 
echo "Test write after runs" >> "$OUTPUT_FILE"

echo "Benchmarks completed. " >&2
cat "$OUTPUT_FILE" >&2

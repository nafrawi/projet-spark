#!/bin/bash

OUTPUT_FILE="/home/nafra/Bureau/code/spark-handson/results/benchmarks.txt"

> "$OUTPUT_FILE"

SCRIPT_DIR="/home/nafra/Bureau/code/spark-handson"

DBT_DIR="/home/nafra/Bureau/code/spark-handson/src/fr/hymaia/exo4/scala"

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

echo "Attempting to change directory to $DBT_DIR" >&2
cd "$DBT_DIR" || { echo "Failed to change directory to $DBT_DIR" >&2; exit 1; }
echo "Running dbt commands in $(pwd)" >&2
echo "Executing: dbt compile" >&2
dbt_compile_results=$(dbt compile 2>&1)
echo "Executing: dbt run" >&2
dbt_run_results=$(dbt run 2>&1)
echo "Logging dbt results to $OUTPUT_FILE" >&2
{
    echo "--------------------------------------------------"
    echo "DBT Commands Output"
    echo "Date: $(date)"
    echo "DBT Compile Results:"
    echo "$dbt_compile_results"
    echo "DBT Run Results:"
    echo "$dbt_run_results"
    echo "--------------------------------------------------"
} >> "$OUTPUT_FILE"
echo "Finished logging dbt commands" >&2
echo "Attempting to change directory back to $SCRIPT_DIR" >&2
cd "$SCRIPT_DIR" || { echo "Failed to change directory to $SCRIPT_DIR" >&2; exit 1; }
poetry run plot 
echo "Test write after runs" >> "$OUTPUT_FILE"
echo "Benchmarks completed." >&2

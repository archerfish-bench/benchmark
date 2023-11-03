#!/bin/bash

# Determine the project home directory based on the script's location
PROJ_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

# Default values
BENCHMARK_CONFIG="$PROJ_HOME/resources/config/benchmark_config.yaml"
BENCHMARK_NAME="WaiiBenchmark"
WORKLOAD_CONFIG="$PROJ_HOME/resources/workloads/spider_workload.yaml"
TEST_SUITE="perf_spider"
RUN_ID="perf_spider_run"
QUERIES_FLAG=""

# Function to display script usage details
display_help() {
    echo "Usage: $0 [options]" >&2
    echo
    echo "   -r, --run_queries         Specify which queries to run, e.g. spider_12,spider_13"
    echo "   -b, --benchmark_config    Path to benchmark config"
    echo "   -n, --benchmark_name      Name of benchmark"
    echo "   -w, --workload_config     Path to workload config"
    echo "   -i, --run_id              Run ID for the benchmark"
    echo "   -h, --help                Display help message"
    echo
    exit 1
}

# Parse command-line arguments
while (( "$#" )); do
  case "$1" in
    -r|--run_queries)
      QUERIES_FLAG="--queries $2"
      shift 2
      ;;
    -b|--benchmark_config)
      BENCHMARK_CONFIG="$2"
      shift 2
      ;;
    -n|--benchmark_name)
      BENCHMARK_NAME="$2"
      shift 2
      ;;
    -w|--workload_config)
      WORKLOAD_CONFIG="$2"
      shift 2
      ;;
    -i|--run_id)
      RUN_ID="$2"
      shift 2
      ;;
    -h|--help)
      display_help
      ;;
    *)
      echo "Error: Invalid argument $1" >&2
      display_help
      ;;
  esac
done

# Check if the env directory exists
if [ ! -d "$PROJ_HOME/env" ]; then
    echo "Virtual environment not found. Creating one now."

    echo "Installing virtualenv"
    python3 -m pip install --user virtualenv
    python3 -m venv "$PROJ_HOME/env"

    echo "Activating virtualenv"
    source "$PROJ_HOME/env/bin/activate"

    echo "Exporting PYTHONPATH"
    export PYTHONPATH="$PROJ_HOME/env/bin:$PROJ_HOME/src"

    echo "Installing requirements"
    pip install -r "$PROJ_HOME/requirements.txt"

    echo "Done with environment setup."
else
    echo "Exporting PYTHONPATH"
    export PYTHONPATH="$PROJ_HOME/env/bin:$PROJ_HOME/src"

    echo "Virtual environment found. Activating."
    source "$PROJ_HOME/env/bin/activate"
fi



# Ensure the script stops on the first error
set -e

echo "Running the benchmark..."

python "$PROJ_HOME/src/benchmark/driver.py" \
  --benchmark_config "$BENCHMARK_CONFIG" \
  --benchmark_name "$BENCHMARK_NAME" \
  --workload_config "$WORKLOAD_CONFIG" \
  $QUERIES_FLAG \
  --run_id "$RUN_ID"

echo "Done!"

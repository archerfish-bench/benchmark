import argparse
import logging
import os
import time
import traceback
from logging.handlers import RotatingFileHandler

import yaml

from gen_report.report_generator import generate_report
from utils.utils import exclude_filter, filter_schema
from utils.utils import include_filter


class Driver:
    """
    Main class for driving the benchmark. This class is responsible for:
    1. Parsing the arguments
    2. Picks up the class mentioned in the benchmark_config.yaml and initializes it.
    3. Runs the benchmark with the specified workload.
    4. Generates the report. Default is html report. User can implement their own report format in gen_report package.
    """

    def __init__(self, benchmark_config: str, benchmark_name: str, workload_config: str, queries: str,
                 exclude_queries: str, run_id: str, schema_filter: str, halt_on_error: bool,
                 report_format='html', intent_based_match=True):
        self._setup_logger()
        self.benchmark_config = benchmark_config
        self.benchmark_name = benchmark_name
        self.queries = queries
        self.workload_config = workload_config
        self.exclude_queries = exclude_queries
        self.run_id = run_id
        self.filter_schema = filter_schema
        self.halt_on_error = halt_on_error
        self.report_format = report_format
        self.intent_based_match = intent_based_match
        try:
            with open(self.benchmark_config, 'r') as f:
                self.benchmark_config_data = yaml.safe_load(f)

            with open(workload_config, 'r') as f:
                data = yaml.safe_load(f)

                # dictionary
                self.workload_data = data

                if schema_filter:
                    self.workload_data = filter_schema(self.workload_data.values(), schema_filter)

                if queries and queries != "*":
                    self.workload_data = include_filter(self.workload_data.values(), queries.split(","))
                if exclude_queries:
                    self.workload_data = exclude_filter(self.workload_data.values(), exclude_queries.split(","))

                logging.info(f"Workload len: {len(self.workload_data)}")

        except FileNotFoundError as e:
            logging.error(f"File not found: {e}")
            print(f"{benchmark_config} file not found: {e}")
            raise e

    def _setup_logger(self):

        # Determine the full path to the logs directory
        logs_directory = os.path.join(os.path.dirname(__file__), "", "../..", "logs")
        os.makedirs(logs_directory, exist_ok=True)  # Create the logs directory if it doesn't exist

        # Specify the log file path within the logs directory
        log_file = os.path.join(logs_directory, "benchmark.log")

        handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] [%(threadName)s]: %(message)s",
            handlers=[
                # logging.StreamHandler(),  # Logs to the console
                handler  # Logs to a file
            ]
        )

    def run(self):
        implementations = self.benchmark_config_data['implementations']
        found = False
        for impl in implementations:
            if impl['name'] == self.benchmark_name:
                print("Got the impl : " + impl['name'])
                self._create_benchmark(impl)
                found = True
                break
        if not found:
            print(
                f"Could not find implementation with name {self.benchmark_name}. Please check config file and arguments")
            return

    def _create_benchmark(self, benchmark_impl: dict):
        class_name = benchmark_impl.get("class")
        description = benchmark_impl.get("description")

        # Dynamically instantiate the class based on the class name
        benchmark = None
        try:
            module_name, class_name = class_name.rsplit('.', 1)
            module = __import__(module_name, fromlist=[class_name])
            class_obj = getattr(module, class_name)
            benchmark = class_obj(workload_data=self.workload_data,
                                  halt_on_error=self.halt_on_error,
                                  intent_based_match=self.intent_based_match)
            print(f"Successfully instantiated the class {class_name}")
        except Exception as e:
            logging.error(f"Failed to instantiate class '{class_name}': {str(e)}")
            print(f"Failed to instantiate class '{class_name}': {str(e)}")

        # Run the benchmark
        if benchmark is not None:
            print("Running the benchmark")
            self._run_benchmark(benchmark, benchmark_impl)
            logging.info(f"Successfully ran benchmark '{description}'")

    def _run_benchmark(self, benchmark_instance, benchmark_config: dict):
        if benchmark_instance is None:
            raise Exception("Benchmark can not be None")
        try:
            benchmark_instance.setup(benchmark_config)
            benchmark_instance.run_benchmark()
            benchmark_instance.cleanup()
        except Exception as e:
            # Handle any exceptions that occur during benchmark execution
            logging.error(f"Error during benchmark:", exc_info=True)
            print(f"Error during benchmark: {str(e)}")
            traceback.print_exc()

        # Generate report
        print("Generating report")
        generate_report(self.run_id, benchmark_instance.get_results(), self.report_format)


def parse_arguments():
    # parse args
    parser = argparse.ArgumentParser(description='Get required fields')

    parser.add_argument('--benchmark_config', type=str,
                        help='config', required=True)
    parser.add_argument('--benchmark_name', type=str,
                        help='name of the implementation', required=True)
    parser.add_argument('--workload_config', type=str,
                        help='workload config', required=True)
    parser.add_argument('--queries', type=str,
                        help='selective queries to be executed', required=False, default='*')
    parser.add_argument('--halt_on_error', type=bool,
                        help="Halt tests on error", required=False, default=False)
    parser.add_argument('--exclude_queries', type=str,
                        help='selective queries to be excluded', required=False,default='')
    parser.add_argument('--run_id', type=str,
                        help='Provide run id for this execution', required=False,
                        default=time.time())
    parser.add_argument('--schema_filter', type=str,
                        help='Only run queries which are targeted for a specific schema',
                        required=False, default='')
    parser.add_argument('--report_format', type=str,
                        help='report format. Default is html. You can implement your own report format',
                        required=False, default='html')
    parser.add_argument('--disable_intent_based_match', action='store_false', dest='intent_based_match',
                        help='Disable intent based match')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()

    driver = Driver(benchmark_config=args.benchmark_config,
                    benchmark_name=args.benchmark_name,
                    workload_config=args.workload_config,
                    queries=args.queries,
                    exclude_queries=args.exclude_queries,
                    run_id=args.run_id,
                    schema_filter=args.schema_filter,
                    halt_on_error=args.halt_on_error,
                    report_format=args.report_format,
                    intent_based_match=args.intent_based_match
                    )
    driver.run()

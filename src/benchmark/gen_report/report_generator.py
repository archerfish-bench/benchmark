import os

import time
from jinja2 import FileSystemLoader, Environment


class BaseReportGenerator:

    def __init__(self):
        self.reports_directory = os.path.join(os.path.dirname(__file__), "..", "..", "..", "reports")

    def generate(self, run_id: str, task_results):
        pass


class HTMLReportGenerator(BaseReportGenerator):
    """
    Generates HTML report using Jinja2 template
    """

    def generate(self, run_id: str, task_results):
        # Generate HTML report using the task_results

        reports_directory = self.reports_directory
        os.makedirs(reports_directory, exist_ok=True)  # Create the reports directory if it doesn't exist

        # Generate the report file name with run_id and timestamp
        time_stamp = time.strftime("%Y-%m-%d-%H-%M-%S")
        report_file_name = f'{run_id}_{time_stamp}.html'

        # Get the absolute path of the report file
        report_file_path = os.path.abspath(os.path.join(reports_directory, report_file_name))

        # Create a Jinja2 environment for rendering the HTML template
        env = Environment(loader=FileSystemLoader(os.path.abspath(os.path.dirname(__file__))))
        template = env.get_template('report_template.html')

        # Add 'round' function to the environment
        env.globals['round'] = round

        # Render the HTML template with task_results
        html_content = template.render(task_results=task_results)

        # Write the HTML content to a report file
        with open(report_file_path, 'w', encoding='utf-8') as report_file:
            report_file.write(html_content)

        print(f'Report file written to: {report_file_path}')


class CSVReportGenerator(BaseReportGenerator):
    def generate(self, run_id: str, task_results):
        # Generate CSV report using the task_results
        pass


class DatabaseReportGenerator(BaseReportGenerator):
    def generate(self, run_id: str, task_results):
        # Store task_results in a database
        pass


def get_report_generator(format):
    if format == 'html':
        return HTMLReportGenerator()
    elif format == 'csv':
        return CSVReportGenerator()
    elif format == 'database':
        return DatabaseReportGenerator()
    else:
        raise ValueError("Unsupported report format")


def generate_report(run_id: str, task_results: list, report_format: str):
    report_generator = get_report_generator(report_format)
    report_generator.generate(run_id, task_results)

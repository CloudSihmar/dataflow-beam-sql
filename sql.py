import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import argparse
import logging
import re
import typing
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms.sql import SqlTransform
from apache_beam.runners.portability import portable_runner
from apache_beam import coders

# Define a NamedTuple for SQL transformation
MyRow = typing.NamedTuple('MyRow', [('date', str), ('product', str), ('sales', int), ('price', float)])
coders.registry.register_coder(MyRow, coders.RowCoder)

def run(p, input_file, output_file):
    # Read from CSV and parse to MyRow NamedTuple
    sales_data = (
        p
        | 'ReadFromGCS' >> ReadFromText(input_file, skip_header_lines=1)
        | 'ParseCSV' >> beam.Map(lambda line: line.split(','))
        | 'ToBeamRow' >> beam.Map(lambda fields: MyRow(fields[0], fields[1], int(fields[2]), float(fields[3])))
    )

    # Define SQL query to calculate total_sales by product
    query = """
        SELECT
            product,
            SUM(sales * price) AS total_sales
        FROM
            PCOLLECTION
        GROUP BY
            product
    """

    # Apply SQL transformation using beam_sql.SqlTransform
    transformed_data = sales_data | 'TransformSalesData' >> SqlTransform(query)

    # Format SQL results and write to output file
    formatted_data = (
        transformed_data
        | 'FormatOutput' >> beam.Map(lambda row: '{}: {}'.format(row.product, row.total_sales))
        | 'WriteToText' >> WriteToText(output_file)
    )

def main():
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='Input file location (CSV format)')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file location')

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        if isinstance(p.runner, portable_runner.PortableRunner):
            # Preemptively start due to BEAM-6666.
            p.runner.create_job_service(pipeline_options)

        run(p, known_args.input, known_args.output)

if __name__ == '__main__':
    main()

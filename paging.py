import csv
import os
import glob
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import click

from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from google.cloud.bigquery_storage_v1beta1.types import (
    ReadSession,
    StreamPosition,
)


class BigQuerySessionConstructor:
    PROJECT = "dl-security-test"

    def __init__(self, stmt, num_streams):
        self.stmt = stmt
        self.num_streams = num_streams
        self.bq_client = bigquery.Client(project=self.PROJECT)

        self.bq_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
        self.run()

    def run(self):
        """Run the query and construct the ReadSession"""
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        self.job = self.bq_client.query(self.stmt, job_config=job_config)

        # wait
        self.job.result()

        # build session
        table_ref = self.job.destination.to_bqstorage()
        parent = "projects/{}".format(self.PROJECT)

        # default number of streams chosen by google to
        # get reasonable read throughput
        self.session = self.bq_storage_client.create_read_session(
            table_ref,
            parent,
            requested_streams=self.num_streams,
            format_=bigquery_storage_v1beta1.enums.DataFormat.ARROW,
        )

    def serialize(self):
        """Serialize the session for later consumption."""
        return self.session.SerializeToString()


class RowReader:
    def __init__(self, session_data):
        self.session = ReadSession.FromString(session_data)

        self.client = bigquery_storage_v1beta1.BigQueryStorageClient()

    def write(self, i, row_iter):
        path = f"data/stream_{i}.csv"
        print(f"starting read for {path}")
        row_count = 0
        write_duration = 0
        with open(path, mode="w") as f:
            writer = csv.writer(f)
            cols = None
            start_time = time.perf_counter()
            for row in row_iter:
                if cols is None:
                    cols = list(sorted(row.keys()))

                out = [row[k] for k in cols]
                write_start = time.perf_counter()
                writer.writerow(out)
                write_duration += time.perf_counter() - write_start
                row_count += 1
            total_duration = time.perf_counter() - start_time
        file_size = to_mb(os.path.getsize(path))
        return (path, row_count, total_duration, write_duration, file_size)

    def read(self):
        stream_positions = [StreamPosition(stream=s)
                            for s in self.session.streams]

        readers = [self.client.read_rows(p) for p in stream_positions]
        iters = [r.rows(self.session) for r in readers]

        rows_iters = []
        with ThreadPoolExecutor(max_workers=len(iters)) as executor:
            futures = {
                executor.submit(self.write, i, iter_): i
                for i, iter_ in enumerate(iters)
            }
            for future in as_completed(futures):
                try:
                    rows = future.result()
                except Exception as exc:
                    i = futures[future]
                    print(f"stream index {i} generated an exception: {exc}")
                else:
                    print(f"stream {rows[0]} complete, got {rows[1]} rows")
                    rows_iters.append(rows)

        return rows_iters


def to_mb(val):
    return val / 1024 ** 2


@click.group()
def cli():
    pass


@cli.command()
@click.argument("path")
@click.option("--num-streams", "-n", type=int, default=0)
def construct(path, num_streams):
    stmt = """
        SELECT *
        FROM `dl-security-test.appsci_spire.vessel_pings`
        LIMIT 20000000
    """
    ctor = BigQuerySessionConstructor(stmt, num_streams)

    session_data = ctor.serialize()

    with open(path, mode="wb") as f:
        f.write(session_data)


@cli.command()
@click.argument("path")
def consume(path):
    files = glob.glob("data/*.csv")
    for file_ in files:
        os.remove(file_)

    with open(path, mode="rb") as f:
        session_data = f.read()

    row_reader = RowReader(session_data)

    print("starting reads")
    start_time = time.perf_counter()
    rows = row_reader.read()
    duration = time.perf_counter() - start_time
    print(f"done reading, took {duration} s wall-clock time")
    total_mb = 0
    total_thread_duration = 0
    total_io_duration = 0
    for row in rows:
        total_mb += row[4]
        total_thread_duration += row[2]
        total_io_duration += row[3]
        print(
            f"{row[0]} got {row[1]} rows: {row[2]} thread time {row[3]} thread time in diskio {row[4]} file size in MiB"
        )

    print(f"{total_mb} total MiB {total_mb/duration} MiB/s")
    print(
        f"{total_mb / (total_thread_duration - total_io_duration)} MiB/s not in disk io"
    )


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    cli()

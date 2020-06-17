from itertools import chain

import click

from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from google.cloud.bigquery_storage_v1beta1.types import (
    ReadSession,
    StreamPosition,
)


class BigQueryReader:
    PROJECT = "dl-security-test"

    def __init__(self, stmt):
        self.stmt = stmt
        self.bq_client = bigquery.Client(project=self.PROJECT)

        self.bq_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
        self.run()

    def run(self):
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        self.job = self.bq_client.query(self.stmt, job_config=job_config)

        # wait
        self.job.result()

        # build session
        table_ref = self.job.destination.to_bqstorage()
        parent = "projects/{}".format(self.PROJECT)
        self.session = self.bq_storage_client.create_read_session(
            table_ref, parent, format_=bigquery_storage_v1beta1.enums.DataFormat.ARROW,
        )

    def serialize(self):
        return self.session.SerializeToString()


class RowReader:
    def __init__(self, session_data):
        self.session = ReadSession.FromString(session_data)

        self.bq_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()

    @property
    def stream_positions(self):
        return [StreamPosition(stream=s) for s in self.session.streams]

    def read(self):
        readers = [self.bq_storage_client.read_rows(
            p) for p in self.stream_positions]

        iters = [r.rows(self.session) for r in readers]
        return chain.from_iterable(iters)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("path")
def construct(path):
    stmt = """
        SELECT *
        -- FROM `dl-security-test.appsci_spire.vessel_pings`;
        FROM `dl-security-test.platform.ports_v1`;
    """
    bq_reader = BigQueryReader(stmt)

    session_data = bq_reader.serialize()

    with open(path, mode="wb") as f:
        f.write(session_data)


@cli.command()
@click.argument("path")
def consume(path):
    with open(path, mode="rb") as f:
        session_data = f.read()

    row_reader = RowReader(session_data)

    rows = list(row_reader.read())
    print(rows[1])


if __name__ == "__main__":
    cli()

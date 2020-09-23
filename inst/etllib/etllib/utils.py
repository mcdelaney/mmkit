import datetime as dt
import logging
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, List
import time
import tempfile
from uuid import uuid1

import google.auth
from google.cloud import bigquery_storage_v1beta1
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from slack.webhook import WebhookClient

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


LOG = logging.getLogger(__name__)
LOG.setLevel(level=logging.INFO)
logFormatter = logging.Formatter(
    "%(asctime)s [%(name)s] [%(levelname)-5.5s] %(message)s")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
LOG.addHandler(consoleHandler)
LOG.propagate = False


class SlackReporter:
    """Class wrapper to send messages to slack datascience-error webhook"""
    def __init__(self):
        self.client = WebhookClient(
            'https://hooks.slack.com/services/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')

    def post_error(self, message):
        self.client.send(text=message)


def access_secret(project_id: str, secret_id: str,
                  version_id: str = 'latest') -> Dict:
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode('UTF-8')
    return json.loads(payload)


class JobInsertError(Exception):
    pass


class Big:
    """Bigquery convenience wrapper."""
    def __init__(self, bucket='emeritus-ds-etl'):
        self.bucket_id = bucket
        bq_creds, project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/drive']
        )
        self.project_id = project_id

        self.bqclient = bigquery.Client(credentials=bq_creds,
                                        project=self.project_id)
        self.bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(
            credentials=bq_creds)

    def query(self, query: str, pydict=False, arrow=False) -> pd.DataFrame:
        """Execute an arbitrary query against bigquery, returning a dataframe."""
        LOG.info('Executing query...')
        result = (
            self.bqclient.query(query)
            .result()
        )

        t1 = time.time()
        if arrow:
                data = result.to_arrow(bqstorage_client=self.bqstorageclient)
        elif pydict:
            data = []
            for elem in result:
                data.append(dict(elem))
            return data
        else:
            data = result.to_dataframe(bqstorage_client=self.bqstorageclient)
        t2 = time.time()
        LOG.info(f'Query returned {data.shape[0]} rows in {round(t2-t1)} secs...')
        return data

    def query_table_fast(self, dataset_name: str, table_name: str,
                        columns: List = [],
                        where_clause = Optional[str],
                        streams: int = 1) -> pd.DataFrame:
        """Dump a table from BQ using the storage API/pyarrow."""
        table = bigquery_storage_v1beta1.types.TableReference()
        table.project_id = self.project_id
        table.dataset_id = dataset_name
        table.table_id = table_name

        # Select columns to read with read options. If no read options are
        # specified, the whole table is read.
        read_options = bigquery_storage_v1beta1.types.TableReadOptions()
        for col in columns:
            read_options.selected_fields.append(col)

        if where_clause:
            read_options.row_restriction = where_clause

        if streams == 1:
            shard = bigquery_storage_v1beta1.enums.ShardingStrategy.LIQUID
        else:
            shard = bigquery_storage_v1beta1.enums.ShardingStrategy.BALANCED

        parent = "projects/{}".format(table.project_id)
        session = self.bqstorageclient.create_read_session(
            table,
            parent,
            requested_streams=streams,
            read_options=read_options,
            format_=bigquery_storage_v1beta1.enums.DataFormat.ARROW,
            # Use LIQUID strategy if reading from only 1 stream.
            sharding_strategy=(shard),
        )

        jobs = []
        for s in range(streams):
            stream = session.streams[s]
            position = bigquery_storage_v1beta1.types.StreamPosition(stream=stream)
            jobs.append((self.bqstorageclient.read_rows(position), session, s))

        def read_rows(r):
            tbl = r[0].to_dataframe(r[1])
            LOG.info(f'Stream {r[2]} complete with {tbl.shape[0]} rows...')
            return tbl

        LOG.info('Executing query against storage api...')
        t1 = time.time()
        with ThreadPoolExecutor(max_workers = streams) as executor:
            results = executor.map(read_rows, jobs)
        tbl = pd.concat(results)
        t2 = time.time()
        LOG.info(f'Rows: {tbl.shape[0]} in {round(t2-t1)} secs...')
        return tbl

    def send(self, sql, job_config=None):
        """Execute a sql query"""
        return self.bqclient.query(sql, job_config)

    def serialize_df_to_gcs_parquet(self, source_tbl: pd.DataFrame, dest_table: str) -> str:
        """
        Serialize a pandas dataframe to parquet, and write to GCS.
        Returns: path to gcs destination object.
        """
        LOG.info(f"Serializing {source_tbl.shape[0]} rows...")
        arrow_tbl = pa.Table.from_pandas(source_tbl, preserve_index=False)
        tmp_file = tempfile.TemporaryFile()
        pq_writer = pq.ParquetWriter(tmp_file, arrow_tbl.schema, version='2.0',
                                     use_deprecated_int96_timestamps=True)
        try:
            pq_writer.write_table(arrow_tbl, 50000)
        except Exception as err:
            LOG.error(f'Serlialiation error: {arrow_tbl.schema}!')
            raise err
        pq_writer.close()
        tmp_file.seek(0)

        LOG.info(f'Uploading file {tmp_file} to gcs...')
        tstamp = dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        client = storage.Client()
        bucket = client.get_bucket(self.bucket_id)
        blob = bucket.blob(f'etl/{dest_table}/{tstamp}.parquet')
        blob.upload_from_file(tmp_file)
        tmp_file.close()
        dest_path = f"gs://{self.bucket_id}/{blob.name}"
        LOG.info(f"Upload complete...destination is: {dest_path}...")
        return dest_path

    def upload_and_write_to_bq(self,
                               source_tbl: pd.DataFrame,
                               dest_table: str,
                               append: bool = False) -> None:
        """Convert pandas to pyarrow/parquet, write write to GCS, then insert."""
        t1 = time.time()
        dest_path = self.serialize_df_to_gcs_parquet(source_tbl, dest_table)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        if append:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        job = self.bqclient.load_table_from_uri(dest_path, dest_table,
                                                job_config=job_config)
        job.result()
        if job.errors:
            LOG.error(job.errors)
            raise JobInsertError
        LOG.info(f'Load job complete in {round(time.time()-t1)} seconds...')

    def merge_insert(self, remote_gcs_path: str, dest_table: str, id_cols=List,
                     tbl_cols=List):
        """Execute merge insert."""
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [remote_gcs_path]
        table_id = 'tmp_' + str(uuid1()).replace('-', '_')
        del_config = bigquery.QueryJobConfig(
            table_definitions={table_id: external_config})
        sql = generate_merge_insert_sql(dest_table, table_id, tbl_cols, id_cols)
        job = self.send(sql, job_config=del_config)
        LOG.info("Starting merge job {}".format(job.job_id))
        job.result()
        if job.errors:
            LOG.error(job.errors)
            raise JobInsertError
        LOG.info('Merge-Load job complete!')

    def insert_or_merge(self, data: pd.DataFrame, dataset: str, table: str,
                        id_cols=List):
        """
        Execute a merge insert on an existing table,
        creating if it doesnt exist.
        """
        try:
            self.bqclient.get_table(f"{dataset}.{table}")
            LOG.info("Table exists...executing merge insert...")
            remote_path = self.serialize_df_to_gcs_parquet(data, f'{dataset}_{table}')
            self.merge_insert(
                dest_table=f'{dataset}.{table}',
                remote_gcs_path=remote_path,
                tbl_cols=list(data.columns),
                id_cols=id_cols
            )
        except NotFound:
            LOG.info("Table does not exist...creating...")
            self.upload_and_write_to_bq(data, f"{dataset}.{table}")


    def execute_statement(self, file_path: str, is_async: bool = False) -> None:
        """
        Execute a sql statement from a file.
        If is_async=True, a job object will be returned.
        Call bq.await_result(result) on this object to block until query completion.
        """
        with open(file_path, 'r') as fp_:
            q = ' '.join(fp_.readlines())
        LOG.info(f"Executing statement from {file_path}...")
        job = self.bqclient.query(q)

        if is_async:
            return job

        self.await_result(job)

    def await_result(self, job):
        """Block until job result returns."""
        job.result()
        if job.errors:
            LOG.error(job.errors)
            raise JobInsertError
        return True


def generate_merge_insert_sql(dest_tbl: str,
                              external_tbl_name: str,
                              tbl_cols: List,
                              id_cols: str) -> str:
    """Execute merge on BigQuery."""
    field_list = ', '.join(tbl_cols)

    update_stmt = []
    for field in tbl_cols:
        if field in id_cols:
            continue
        update_stmt.append(f"{field} = S.{field}")
    update_stmt = ', '.join(update_stmt)

    merge_keys = " = ".join([ "CONCAT( " + " || ".join([f"IFNULL(CAST({t}.{f} AS STRING), '__NULL_VAL_999_')"
                                           for f in id_cols]) + " )"
                 for t in ["T", "S"]])
    # merge_keys = " AND ".join([f"COALESCE(T.{id_col}, '') = COALESCE(S.{id_col}, '')"
                                # for id_col in id_cols])
    sql = f"""MERGE {dest_tbl} T
            USING {external_tbl_name} S
                ON {merge_keys}
            WHEN MATCHED THEN
                UPDATE SET {update_stmt}
            WHEN NOT MATCHED THEN
                INSERT ({field_list}) VALUES ({field_list})
    """
    return sql


def get_param(param_dict: Dict, param: str) -> Optional[str]:
    """Extract a named query param, returning none if missing."""
    try:
        return param_dict[param][0]
    except (KeyError, IndexError):
        return None

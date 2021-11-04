# built-in
import datetime as dt
from io import BytesIO
import json
import logging
import os
from pathlib import Path
from time import sleep
from random import randrange

# External
from azure.storage.blob import BlobServiceClient
from jinja2 import Environment, FileSystemLoader
import pyodbc  # type: ignore


class ConnectionManager:
    """
    Context manager class for the database connection.
    """
    def __init__(self) -> None:
        self.conn_string = os.environ['SERVERLESS_DB']

    def __enter__(self) -> pyodbc.Connection:
        i = 1
        while i < 7:
            logging.info(f'Connecting to DB; time: {i}')
            try:
                self.con = pyodbc.connect(self.conn_string)
            except pyodbc.DatabaseError:
                sleep(10)
                i += 1
            else:
                return self.con

    def __exit__(self, *args, **kwargs) -> None:
        self.con.close()


class ContentHandler:
    def __init__(self, uid: int) -> None:
        self.uid = uid

    def get_content(self, cur: pyodbc.Cursor):
        run_ts = dt.datetime.now()
        table_rows = []
        cur.execute(
            "SELECT name, value FROM source WHERE uid = ?;",
            (self.uid)
        )
        row = cur.fetchone()
        name = row.name
        table_rows.append(
            {
                'run_ts': run_ts.isoformat(sep=' '),
                'value': row.value + randrange(0, 21)
            }
        )
        cur.execute(
            "SELECT TOP 1 content_json FROM history\n"
            "WHERE uid = ? ORDER BY run_ts DESC;",
            (self.uid)
        )
        row = cur.fetchone()
        if isinstance(row, pyodbc.Row):
            table_rows += json.loads(row.content_json)
        cur.execute(
            "INSERT INTO history (uid, run_ts, content_json)\n"
            "VALUES (?, ?, ?);",
            (self.uid, run_ts, json.dumps(table_rows))
        )
        cur.commit()
        return (self.uid, name, table_rows)


class HTMLMaker:
    container_map = {
        99: '99-fau',
        100: '100-fau'
    }
    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ["HTML_ASA_CONN_STRING"]
    )
    file_dir = Path(__file__).parent
    env = Environment(
        loader=FileSystemLoader(
            file_dir.joinpath('templates'),
            encoding='utf-8'
        ),
        trim_blocks=True,
        lstrip_blocks=True
    )
    tmplt = env.get_template('template.html')
    cur = None

    def __init__(self, uid: int) -> None:
        self.uid = uid

    @classmethod
    def set_cursor(self, cur: pyodbc.Cursor) -> None:
        self.cur = cur

    def make(self) -> None:
        agent = ContentHandler(self.uid)
        uid, name, table_rows = agent.get_content(self.cur)
        html_content = self.tmplt.render(
            uid=uid,
            name=name,
            table_rows=table_rows
        )
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_map[self.uid],
            blob=f'{len(table_rows)}.html'
        )
        payload = BytesIO(html_content.encode('utf-8'))
        blob_client.upload_blob(payload)
        payload.close()

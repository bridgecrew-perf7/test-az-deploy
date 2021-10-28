# built-in
import datetime as dt
import json
from pathlib import Path
import os
from random import randrange
from typing import List, Optional, Tuple

# External
from jinja2 import Environment, FileSystemLoader
from mypy_extensions import TypedDict
import pyodbc  # type: ignore

TableRow = TypedDict(
    'TableRow',
    {
        'run_ts': str,
        'value': int
    }
)
TemplateInput = Tuple[int, str, List[TableRow]]


class ConnectionManager:
    """
    Context manager class for the database connection.
    """
    def __init__(self) -> None:
        self.conn_string = os.environ['SERVERLESS_DB']

    def __enter__(self) -> pyodbc.Connection:
        self.con = pyodbc.connect(self.conn_string)
        return self.con

    def __exit__(self, *args, **kwargs) -> None:
        self.con.close()


class ContentHandler:
    run_ts = dt.datetime.now()

    def __init__(self, uid: int) -> None:
        self.uid = uid

    def get_content(self, cur: pyodbc.Cursor) -> TemplateInput:
        table_rows: List[TableRow] = []
        cur.execute(
            "SELECT name, value FROM source WHERE uid = ?;",
            (self.uid)
        )
        row = cur.fetchone()
        name = row.name
        table_rows.append(
            {
                'run_ts': self.run_ts.isoformat(sep=' '),
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
            (self.uid, self.run_ts, json.dumps(table_rows))
        )
        cur.commit()
        return (self.uid, name, table_rows)


class HTMLMaker:
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
    cur: Optional[pyodbc.Cursor] = None
    out_dir: Optional[Path] = None

    def __init__(self, uid: int) -> None:
        self.uid = uid

    @classmethod
    def set_cursor(self, cur: pyodbc.Cursor) -> None:
        self.cur = cur

    @classmethod
    def set_out_dir(self, out_dir: Path) -> None:
        self.out_dir = out_dir

    def make(self) -> None:
        agent = ContentHandler(self.uid)
        uid, name, table_rows = agent.get_content(self.cur)
        html_content = self.tmplt.render(
            uid=uid,
            name=name,
            table_rows=table_rows
        )
        outpath = self.out_dir.joinpath(  # type: ignore[union-attr]
            str(self.uid), f"{len(table_rows)}.html"
        )
        subfolder = outpath.parent
        if not subfolder.exists():
            subfolder.mkdir()
        with open(outpath, mode='w', encoding='utf-8') as fw:
            fw.write(html_content)

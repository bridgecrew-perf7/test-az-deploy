# Built-in
import logging

# External
import azure.functions as func

# Own
from .workers import ConnectionManager, HTMLMaker


def main(mytimer: func.TimerRequest) -> None:
    logging.info('The timer is past due!')
    with ConnectionManager() as con:
        cur = con.cursor()
        cur.execute("SELECT uid FROM source;")
        uid_iter = map(lambda x: x.uid, cur.fetchall())
        HTMLMaker.set_cursor(cur)
        for uid in uid_iter:
            agent = HTMLMaker(uid)
            agent.make()

    logging.info('Python timer trigger function ran at %s', str(mytimer))

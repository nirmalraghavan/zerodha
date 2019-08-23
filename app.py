import csv
import time
from datetime import date
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile

import cherrypy
import requests
from redis import Redis


class StockData(object):
    @cherrypy.expose
    def update(self):
        # Build file URL.
        filename = f"EQ{date.today().strftime('%d%m%y')}"
        bhav_copy = f'https://www.bseindia.com/download/BhavCopy/Equity/{filename}_CSV.ZIP'

        # Parse CSV file.
        with ZipFile(BytesIO(requests.get(bhav_copy).content)) as bhav_copy:
            with bhav_copy.open(f'{filename}.CSV') as csv_file:
                # Insert CSV entries.
                csv_reader = csv.reader(TextIOWrapper(csv_file))
                next(csv_reader)
                for row in csv_reader:
                    # Some entries contains 0 for previous close.
                    try:
                        change = ((float(row[8]) - float(row[9])) * 100) / float(row[9])
                    except ZeroDivisionError:
                        change = 0

                    # Push to redis.
                    db.hmset(row[1].strip(), {
                        'code': row[0],
                        'open': row[4],
                        'high': row[5],
                        'low': row[6],
                        'close': row[7],
                        'change': change
                    })

                # Add timestamp.
                db.set('last_updated', time.time())
        return 'completed'

    @cherrypy.expose
    def shutdown(self):
        cherrypy.engine.exit()

    @cherrypy.expose
    def index(self):
        return "Hello World"


if __name__ == '__main__':
    # Connect to Redis DB.
    db = Redis('localhost', 6379)

    # Start server.
    cherrypy.quickstart(StockData())

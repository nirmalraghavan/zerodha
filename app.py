import csv
import os
from datetime import datetime, date
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile

import cherrypy
import redis
import requests
from jinja2 import Environment, FileSystemLoader


# Server configuration.
config = {
    'global': {
        'server.socket_host': '0.0.0.0',
        'server.socket_port': int(os.environ.get('PORT', 8080)),
    }
}


class StockData(object):
    """Handles table population and data update.
    """
    @cherrypy.expose
    def update(self):
        # Build file URL.
        filename = f"EQ{date.today().strftime('%d%m%y')}"
        bhav_copy = requests.get(f'https://www.bseindia.com/download/BhavCopy/Equity/{filename}_CSV.ZIP')

        # Fail over to older CSV.
        if not bhav_copy.ok:
            filename = 'EQ230819'
            bhav_copy = requests.get(f'https://www.bseindia.com/download/BhavCopy/Equity/{filename}_CSV.ZIP')

        # Parse CSV file.
        with ZipFile(BytesIO(bhav_copy.content)) as bhav_copy:
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
                    stock_name = row[1].strip()
                    db.hmset(f'stock:{stock_name}', {
                        'name': stock_name,
                        'code': row[0],
                        'open': row[4],
                        'high': row[5],
                        'low': row[6],
                        'close': row[7],
                        'prev_close': row[9],
                        'change': change
                    })
                    db.sadd('stock', f'stock:{stock_name}')

                # Add timestamp.
                db.set('last_updated', datetime.now().timestamp())
        raise cherrypy.HTTPRedirect('/')

    @cherrypy.expose
    def shutdown(self):
        cherrypy.engine.exit()

    @cherrypy.expose
    def index(self, q=None):
        if q:
            # Search for matching stocks.
            stocks = []
            for key in db.scan_iter(f'stock:{q.upper()}*'):
                stock = db.hgetall(key)
                stocks.append((
                    stock['name'], stock['code'], stock['open'], stock['high'], stock['low'], stock['close'],
                    stock['prev_close'], stock['change']))
        else:
            # Get top 10 stocks.
            stocks = db.sort(
                'stock', by='*->change', get=(
                    '*->name', '*->code', '*->open', '*->high', '*->low', '*->close', '*->prev_close', '*->change'),
                desc=True, groups=True, start=0, num=10)

        # Load template.
        template = Environment(loader=FileSystemLoader('templates')).get_template('index.html')

        # Get last updated.
        last_updated = db.get('last_updated')
        if last_updated:
            last_updated = datetime.fromtimestamp(float()).isoformat()
        else:
            last_updated = 'No data found. Please sync now.'

        # Render template.
        return template.render(last_updated=last_updated, stocks=stocks, q=q or '')


if __name__ == '__main__':
    # Connect to Redis DB.
    db = redis.from_url(os.environ.get("REDIS_URL"), decode_responses=True)

    # Start server.
    cherrypy.quickstart(StockData(), config=config)

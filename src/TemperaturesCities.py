import datetime

import psycopg2
from flask_restful import Resource, reqparse


class TemperaturesCities(Resource):
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.db = kwargs['db']

    def get(self, id):
        parser = reqparse.RequestParser()
        parser.add_argument('from', default=None, required=False, type=str, location='args')
        parser.add_argument('until', default=None, required=False, type=str, location='args')

        args = parser.parse_args()
        args['id'] = id
        if args['from']:
            try:
                args['from'] = datetime.datetime.strptime(args['from'], '%Y-%m-%d')
            except ValueError:
                self.logger.error(str(args['from']) + ' not a YYYY-MM-DD date')
                return {}, 400
        if args['until']:
            try:
                args['until'] = datetime.datetime.strptime(args['until'], '%Y-%m-%d')
            except ValueError:
                self.logger.error(str(args['until']) + ' not a YYYY-MM-DD date')
                return {}, 400

        cur = self.db.cursor()
        try:
            cur.execute("""
                        SELECT id, valoare, timestamp FROM Temperaturi
                        WHERE id_oras = %(id)s
                          AND (%(from)s IS NULL OR timestamp >= %(from)s)
                          AND (%(until)s IS NULL OR timestamp <= %(until)s);
                        """,
                        args)
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.logger.error('query with args ' + str(args) + ' failed')
            return {}, 400

        rows = cur.fetchall()
        cur.close()

        # Format response as a list of dictionaries
        cols = ['id', 'valoare', 'timestamp']
        rows = [dict(zip(cols, row)) for row in rows]
        for row in rows:
            row['timestamp'] = row['timestamp'].strftime("%Y-%m-%d")
        self.logger.info('Selected rows: ' + str(rows))

        return rows, 200

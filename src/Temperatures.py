import datetime

import psycopg2
from flask import request
from flask_restful import Resource, reqparse


class Temperatures(Resource):
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.db = kwargs['db']

    # Check that the request body is correctly formatted as a temperature
    def check_temperature(self, req):
        if 'idOras' not in req or 'valoare' not in req:
            self.logger.warning('request ' + str(req) + ' does not contain a temperature')
            return 400

        try:
            float(req['idOras'])
        except ValueError:
            self.logger.warning('in request ' + str(req) + ', idOras is not an integer')
            return 400

        try:
            float(req['valoare'])
        except ValueError:
            self.logger.warning('in request ' + str(req) + ', valoare is not a real number')
            return 400

        return None

    # Check that the request body is correctly formatted for POST request
    def check_post(self, req):
        if len(req) != 2:
            self.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400

        return self.check_temperature(req)

    # Check that the request body is correctly formatted for PUT request
    def check_put(self, req):

        if len(req) != 3:
            self.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400
        if 'id' not in req:
            self.logger.warning('request ' + str(req) + ' does not contain an id')
            return 400
        try:
            int(req['id'])
        except ValueError:
            self.logger.warning('in request ' + str(req) + ', id is not an integer')
            return 400

        return self.check_temperature(req)

    def post(self):
        temp = request.get_json()
        err = self.check_post(temp)
        if err:
            return {}, err

        # Execute INSERT SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        INSERT INTO Temperaturi(valoare, timestamp, id_oras)
                        VALUES (%(valoare)s, current_timestamp, %(idOras)s)
                        RETURNING id;
                        """,
                        temp)
        except psycopg2.errors.UniqueViolation:
            cur.close()
            self.db.rollback()
            self.logger.warning(
                'Temperature for ' + str((temp['idOras'], temp['timestamp'])) + ' already exists in table')
            return {}, 409
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(temp) + ' could not be inserted in database')
            return {}, 400

        # Retrieve ID of the inserted temperature, if the insert was succesful
        try:
            temp_id = cur.fetchone()[0]
        except psycopg2.ProgrammingError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(temp) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        self.db.commit()

        self.logger.info(str(temp) + ' was inserted in database with id ' + str(temp_id))
        return {'id': temp_id}, 201

    def put(self, id):
        temp = request.get_json()
        err = self.check_put(temp)
        if err:
            return {}, err

        # Execute UPDATE SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        UPDATE Temperaturi
                        SET id = %s, id_oras = %s, valoare = %s
                        WHERE id = %s;
                        """,
                        (temp['id'], temp['idOras'], temp['valoare'], id))
        except psycopg2.errors.DataError:
            cur.close()
            self.db.rollback()
            self.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(temp) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        self.db.commit()

        self.logger.info('temperature with id=' + str(id) + ' was updated in database to ' + str(temp))
        return {}, 200

    def delete(self, id):
        # Execute DELETE SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        DELETE FROM Temperaturi
                        WHERE id = %s;
                        """,
                        (id,))
        except psycopg2.errors.DataError:
            cur.close()
            self.db.rollback()
            self.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.db.rollback()
            self.logger.error('id=' + str(id) + ' could not be deleted from database')
            return {}, 400

        cur.close()
        self.db.commit()

        self.logger.info('country with id=' + str(id) + ' was deleted from database')
        return {}, 200

    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('lat', default=None, required=False, type=float, location='args')
        parser.add_argument('lon', default=None, required=False, type=float, location='args')
        parser.add_argument('from', default=None, required=False, type=str, location='args')
        parser.add_argument('until', default=None, required=False, type=str, location='args')

        args = parser.parse_args()
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
                        SELECT t.id, t.valoare, t.timestamp FROM Temperaturi t JOIN Orase o ON t.id_oras = o.id
                        WHERE (%(lat)s IS NULL OR abs(o.latitudine - %(lat)s) < 0.001)
                          AND (%(lon)s IS NULL OR abs(o.longitudine - %(lon)s) < 0.001)
                          AND (%(from)s IS NULL OR t.timestamp >= %(from)s)
                          AND (%(until)s IS NULL OR t.timestamp <= %(until)s);
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

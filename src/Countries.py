import psycopg2
from flask import request
from flask_restful import Resource


class Countries(Resource):
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.db = kwargs['db']

    # Check that the request body is correctly formatted as a country
    def check_country(self, req):
        if 'nume' not in req or 'lat' not in req or 'lon' not in req:
            self.logger.warning('request ' + str(req) + ' does not contain a country')
            return 400

        try:
            float(req['lat'])
        except ValueError:
            self.logger.warning('in request ' + str(req) + ', latitude is not a real number')
            return 400

        try:
            float(req['lon'])
        except ValueError:
            self.logger.warning('in request ' + str(req) + ', longitude is not a real number')
            return 400

        return None

    # Check that the request body is correctly formatted for POST request
    def check_post(self, req):
        if len(req) != 3:
            self.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400

        return self.check_country(req)

    # Check that the request body is correctly formatted for PUT request
    def check_put(self, req):
        if len(req) != 4:
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

        return self.check_country(req)

    def post(self):
        country = request.get_json()
        err = self.check_post(country)
        if err:
            return {}, err

        # Execute INSERT SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        INSERT INTO Tari(nume_tara, latitudine, longitudine)
                        VALUES (%(nume)s, %(lat)s, %(lon)s)
                        RETURNING id;
                        """,
                        country)
        except psycopg2.errors.UniqueViolation:
            cur.close()
            self.db.rollback()
            self.logger.warning(country['nume'] + ' already exists in table')
            return {}, 409
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(country) + ' could not be inserted in database')
            return {}, 409

        # Retrieve ID of country, if the insert was succesful
        try:
            country_id = cur.fetchone()[0]
        except psycopg2.ProgrammingError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(country) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        self.db.commit()

        self.logger.info(str(country) + ' was inserted in database with id ' + str(country_id))
        return {'id': country_id}, 201

    def get(self):
        cur = self.db.cursor()
        cur.execute('SELECT * FROM Tari;')
        rows = cur.fetchall()
        cur.close()

        # Format response as a list of dictionaries
        cols = ['id', 'nume', 'lat', 'lon']
        rows = [dict(zip(cols, row)) for row in rows]
        self.logger.info('Selected rows: ' + str(rows))

        return rows, 200

    def put(self, id):
        country = request.get_json()
        err = self.check_put(country)
        if err:
            return {}, err

        # Execute UPDATE SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        UPDATE Tari
                        SET id = %s, nume_tara = %s, latitudine = %s, longitudine = %s
                        WHERE id = %s;
                        """,
                        (country['id'], country['nume'], country['lat'], country['lon'], id))
        except psycopg2.errors.DataError:
            cur.close()
            self.db.rollback()
            self.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(country) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        self.db.commit()

        self.logger.info('country with id=' + str(id) + ' was updated in database to ' + str(country))
        return {}, 200

    def delete(self, id):
        # Execute DELETE SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        DELETE FROM Tari
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

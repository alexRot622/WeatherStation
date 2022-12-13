import psycopg2
from flask import request
from flask_restful import Resource


class Cities(Resource):
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.db = kwargs['db']

    # Check that the request body is correctly formatted as a city
    def check_city(self, req):
        if 'idTara' not in req or 'nume' not in req or 'lat' not in req or 'lon' not in req:
            self.logger.warning('request ' + str(req) + ' does not contain a city')
            return 400

        try:
            float(req['idTara'])
        except ValueError:
            self.logger.warning('in request ' + str(req) + ', idTara is not an integer')
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
        if len(req) != 4:
            self.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400

        return self.check_city(req)

    # Check that the request body is correctly formatted for PUT request
    def check_put(self, req):
        if len(req) != 5:
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

        return self.check_city(req)

    def post(self):
        city = request.get_json()
        err = self.check_post(city)
        if err:
            return {}, err

        # Execute INSERT SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        INSERT INTO Orase(id_tara, nume_oras, latitudine, longitudine)
                        VALUES (%(idTara)s, %(nume)s, %(lat)s, %(lon)s)
                        RETURNING id;
                        """,
                        city)
        except psycopg2.errors.UniqueViolation:
            cur.close()
            self.db.rollback()
            self.logger.warning(city['nume'] + ' already exists in table')
            return {}, 409
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(city) + ' could not be inserted in database')
            return {}, 409

        # Retrieve ID of city, if the insert was succesful
        try:
            city_id = cur.fetchone()[0]
        except psycopg2.ProgrammingError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(city) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        self.db.commit()

        self.logger.info(str(city) + ' was inserted in database with id ' + str(city_id))
        return {'id': city_id}, 201

    def get(self):
        cur = self.db.cursor()
        cur.execute('SELECT * FROM Orase;')
        rows = cur.fetchall()
        cur.close()

        # Format response as a list of dictionaries
        cols = ['id', 'idTara', 'nume', 'lat', 'lon']
        rows = [dict(zip(cols, row)) for row in rows]
        self.logger.info('Selected rows: ' + str(rows))

        return rows, 200

    def put(self, id):
        city = request.get_json()
        err = self.check_put(city)
        if err:
            return {}, err

        # Execute UPDATE SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        UPDATE Orase
                        SET id = %s, id_tara = %s, nume_oras = %s, latitudine = %s, longitudine = %s
                        WHERE id = %s;
                        """,
                        (city['id'], city['idTara'], city['nume'], city['lat'], city['lon'], id))
        except psycopg2.errors.DataError:
            cur.close()
            self.db.rollback()
            self.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            self.db.rollback()
            self.logger.error(str(city) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        self.db.commit()

        self.logger.info('city with id=' + str(id) + ' was updated in database to ' + str(city))
        return {}, 200

    def delete(self, id):
        # Execute DELETE SQL operation
        cur = self.db.cursor()
        try:
            cur.execute("""
                        DELETE FROM Orase
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

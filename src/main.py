import logging
from flask import Flask, request
from flask_restful import Resource, Api
from time import sleep
import psycopg2

def getConnection():
    return psycopg2.connect(
        host='postgresql',
        database='measurements',
        user='pgsql',
        password='pgsql'
    )

class Countries(Resource):
    # Check that the request body is correctly formatted as a country
    def checkCountry(req):
        global app

        if 'nume' not in req or 'lat' not in req or 'lon' not in req:
            app.logger.warning('request ' + str(req) + ' does not contain a country')
            return 400

        try:
            float(req['lat'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', latitude is not a real number')
            return 400

        try:
            float(req['lon'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', longitude is not a real number')
            return 400

        return None


    # Check that the request body is correctly formatted for POST request
    def checkPostRequest(req):
        global app

        if len(req) != 3:
            app.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400

        return Countries.checkCountry(req)


    # Check that the request body is correctly formatted for PUT request
    def checkPutRequest(req):
        global app

        if len(req) != 4:
            app.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400
        if 'id' not in req:
            app.logger.warning('request ' + str(req) + ' does not contain an id')
            return 400
        try:
            int(req['id'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', id is not an integer')

        return Countries.checkCountry(req)


    def post(self):
        global app

        country = request.get_json()
        err = Countries.checkPostRequest(country)
        if err:
            return {}, err

        # Execute INSERT SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        INSERT INTO Tari(nume_tara, latitudine, longitudine)
                        VALUES (%(nume)s, %(lat)s, %(lon)s)
                        RETURNING id;
                        """,
                        country)
        except psycopg2.errors.UniqueViolation:
            cur.close()
            conn.rollback()
            app.logger.warning(country['nume'] + ' already exists in table')
            return {}, 409
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(country) + ' could not be inserted in database')
            return {}, 409

        # Retrieve ID of country, if the insert was succesful
        try:
            country_id = cur.fetchone()
        except psycopg2.ProgrammingError:
            cur.close()
            conn.rollback()
            app.logger.error(str(country) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info(str(country) + ' was inserted in database with id ' + str(country_id))
        return country_id, 201


    def get(self):
        global app
        global conn

        cur = conn.cursor()
        cur.execute('SELECT * FROM Tari;')
        rows = cur.fetchall()
        cur.close()

        return rows, 200


    def put(self, id):
        global app

        # TODO: try: is id int? then convert to int
        country = request.get_json()
        err = Countries.checkPutRequest(country)
        if err:
            return {}, err

        # Execute UPDATE SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        UPDATE Tari
                        SET id = %s, nume = %s, latitudine = %s, longitudine = %s
                        WHERE id = %s
                        """,
                        (country['id'], country['nume'], country['lat'], country['lon'], id))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(country) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('country with id='str(id) + ' was updated in database to ' + str(country))
        return {}, 200


    def delete(self, id):
        global app

        # TODO: try: is id int? then convert to int

        # Execute DELETE SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        DELETE FROM Tari
                        WHERE id = %s
                        """,
                        (id))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(country) + ' could not be deleted from database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('country with id='str(id) + ' was deleted from database')
        return {}, 200


class Cities(Resource):
    # Check that the request body is correctly formatted as a city
    def checkCity(req):
        global app

        if 'idTara' not in req or 'nume' not in req or 'lat' not in req or 'lon' not in req:
            app.logger.warning('request ' + str(req) + ' does not contain a city')
            return 400

        try:
            float(req['idTara'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', idTara is not an integer')
            return 400

        try:
            float(req['lat'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', latitude is not a real number')
            return 400

        try:
            float(req['lon'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', longitude is not a real number')
            return 400

        return None


    # Check that the request body is correctly formatted for POST request
    def checkPostRequest(req):
        global app

        if len(req) != 4:
            app.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400

        return Cities.checkCountry(req)


    # Check that the request body is correctly formatted for PUT request
    def checkPutRequest(req):
        global app

        if len(req) != 5:
            app.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400
        if 'id' not in req:
            app.logger.warning('request ' + str(req) + ' does not contain an id')
            return 400
        try:
            int(req['id'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', id is not an integer')

    def post(self):
        global app

        city = request.get_json()
        err = Cities.checkPostRequest(city)
        if err:
            return {}, err

        # Execute INSERT SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        INSERT INTO Orase(id_tara, nume, latitudine, longitudine)
                        VALUES (%(idTara)s, %(nume)s, %(lat)s, %(lon)s)
                        RETURNING id;
                        """,
                        city)
        except psycopg2.errors.UniqueViolation:
            cur.close()
            conn.rollback()
            app.logger.warning(city['nume'] + ' already exists in table')
            return {}, 409
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(city) + ' could not be inserted in database')
            return {}, 409

        # Retrieve ID of city, if the insert was succesful
        try:
            city_id = cur.fetchone()
        except psycopg2.ProgrammingError:
            cur.close()
            conn.rollback()
            app.logger.error(str(city) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info(str(city) + ' was inserted in database with id ' + str(city_id))
        return city_id, 201


    def get(self):
        global app
        global conn

        cur = conn.cursor()
        cur.execute('SELECT * FROM Orase;')
        rows = cur.fetchall()
        cur.close()

        return rows, 200

    
    def get(self, id):
        global app

        # TODO: try: is id int? then convert to int

        global conn

        cur = conn.cursor()
        cur.execute("""
                    SELECT * FROM Orase
                    WHERE idTara = %s;
                    """,
                    (id))
        rows = cur.fetchall()
        cur.close()

        return rows, 200
    

    def put(self, id):
        global app

        # TODO: try: is id int? then convert to int
        city = request.get_json()
        err = Cities.checkPutRequest(country)
        if err:
            return {}, err

        # Execute UPDATE SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        UPDATE Orase
                        SET id = %s, idTara = %s, nume = %s, latitudine = %s, longitudine = %s
                        WHERE id = %s
                        """,
                        (city['id'], city['idTara'], city['nume'], city['lat'], city['lon'], id))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(country) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('city with id='str(id) + ' was updated in database to ' + str(city))
        return {}, 200


    def delete(self, id):
        global app

        # TODO: try: is id int? then convert to int

        # Execute DELETE SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        DELETE FROM Tari
                        WHERE id = %s
                        """,
                        (id))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(city) + ' could not be deleted from database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('country with id='str(id) + ' was deleted from database')
        return {}, 200

# TODO: Close database cursors and connection during shutdown
# TODO: Health check in docker compose before connection to DB
# TODO: Triggers for delete

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app = Flask(__name__)
    api = Api(app)

    app.logger.info('Waiting for database start up')
    sleep(10.0) #TODO: Replace this with health check

    app.logger.info('Connecting to database')
    conn = getConnection()

    api.add_resource(Countries, '/api/countries')
    api.add_resource(Countries, '/api/countries/<id>')

    api.add_resource(Countries, '/api/cities')
    api.add_resource(Countries, '/api/cities/<id>')

    app.run(host='0.0.0.0', port=5000)

import logging
from flask import Flask, request
from flask_restful import Resource, Api, reqparse
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
            return 400

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
            country_id = cur.fetchone()[0]
        except psycopg2.ProgrammingError:
            cur.close()
            conn.rollback()
            app.logger.error(str(country) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info(str(country) + ' was inserted in database with id ' + str(country_id))
        return {'id': country_id}, 201


    def get(self):
        global app
        global conn

        cur = conn.cursor()
        cur.execute('SELECT * FROM Tari;')
        rows = cur.fetchall()
        cur.close()

        # Format response as a list of dictionaries
        cols = ['id', 'nume', 'lat', 'lon']
        rows = [dict(zip(cols, row)) for row in rows]

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
                        WHERE id = %s;
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

        app.logger.info('country with id=' + str(id) + ' was updated in database to ' + str(country))
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
                        WHERE id = %s;
                        """,
                        (id,))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error('id=' + str(id) + ' could not be deleted from database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('country with id=' + str(id) + ' was deleted from database')
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

        return Cities.checkCity(req)


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
            return 400
        
        return Cities.checkCity(req)


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
            city_id = cur.fetchone()[0]
        except psycopg2.ProgrammingError:
            cur.close()
            conn.rollback()
            app.logger.error(str(city) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info(str(city) + ' was inserted in database with id ' + str(city_id))
        return {'id': city_id}, 201


    def get(self):
        global app
        global conn

        cur = conn.cursor()
        cur.execute('SELECT * FROM Orase;')
        rows = cur.fetchall()
        cur.close()

        # Format response as a list of dictionaries
        cols = ['id', 'idTara', 'nume', 'lat', 'lon']
        rows = [dict(zip(cols, row)) for row in rows]

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
                        SET id = %s, id_tara = %s, nume = %s, latitudine = %s, longitudine = %s
                        WHERE id = %s;
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

        app.logger.info('city with id=' + str(id) + ' was updated in database to ' + str(city))
        return {}, 200


    def delete(self, id):
        global app

        # TODO: try: is id int? then convert to int

        # Execute DELETE SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        DELETE FROM Orase
                        WHERE id = %s;
                        """,
                        (id,))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error('id=' + str(id) + ' could not be deleted from database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('country with id=' + str(id) + ' was deleted from database')
        return {}, 200

class CitiesCountry(Resource):
    def get(self, id):
        global app

        # TODO: try: is id int? then convert to int

        global conn

        cur = conn.cursor()
        cur.execute("""
                    SELECT * FROM Orase
                    WHERE id_tara = %s;
                    """,
                    (id,))
        rows = cur.fetchall()
        cur.close()

        # Format response as a list of dictionaries
        cols = ['id', 'idTara', 'nume', 'lat', 'lon']
        rows = [dict(zip(cols, row)) for row in rows]

        return rows, 200

class Temperatures(Resource):
    # Check that the request body is correctly formatted as a city
    def checkTemperature(req):
        global app

        if 'idOras' not in req or 'valoare' not in req:
            app.logger.warning('request ' + str(req) + ' does not contain a temperature')
            return 400

        try:
            float(req['idOras'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', idOras is not an integer')
            return 400

        try:
            float(req['valoare'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', valoare is not a real number')
            return 400

        return None


    # Check that the request body is correctly formatted for POST request
    def checkPostRequest(req):
        global app

        if len(req) != 2:
            app.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400

        return Cities.checkCity(req)


    # Check that the request body is correctly formatted for PUT request
    def checkPutRequest(req):
        global app

        if len(req) != 3:
            app.logger.warning('request ' + str(req) + ' does not contain correct number of parameters')
            return 400
        if 'id' not in req:
            app.logger.warning('request ' + str(req) + ' does not contain an id')
            return 400
        try:
            int(req['id'])
        except ValueError:
            app.logger.warning('in request ' + str(req) + ', id is not an integer')
            return 400

        return Cities.checkCity(req)

    def post(self):
        global app

        temp = request.get_json()
        err = Temperatures.checkPostRequest(temp)
        if err:
            return {}, err

        # Execute INSERT SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        INSERT INTO Temperaturi(valoare, timestamp, id_oras)
                        VALUES (%(valoare)s, %(timestamp)s, %(idOras)s)
                        RETURNING id;
                        """,
                        temp)
        except psycopg2.errors.UniqueViolation:
            cur.close()
            conn.rollback()
            app.logger.warning('Temperature for ' + str((temp['id_oras'], temp['timestamp'])) + ' already exists in table')
            return {}, 409
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(temp) + ' could not be inserted in database')
            return {}, 409

        # Retrieve ID of the inserted temperature, if the insert was succesful
        try:
            temp_id = cur.fetchone()[0]
        except psycopg2.ProgrammingError:
            cur.close()
            conn.rollback()
            app.logger.error(str(temp) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info(str(temp) + ' was inserted in database with id ' + str(temp_id))
        return {'id': temp_id}, 201


    def put(self, id):
        global app

        # TODO: try: is id int? then convert to int
        temp = request.get_json()
        err = Cities.checkPutRequest(temp)
        if err:
            return {}, err

        # Execute UPDATE SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        UPDATE Temperaturi
                        SET id = %s, id_oras = %s, valoare = %s
                        WHERE id = %s;
                        """,
                        (temp['id'], temp['id_oras'], temp['id_oras'], id))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error(str(temp) + ' could not be inserted in database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('temperature with id=' + str(id) + ' was updated in database to ' + str(temp))
        return {}, 200


    def delete(self, id):
        global app

        # TODO: try: is id int? then convert to int

        # Execute DELETE SQL operation
        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        DELETE FROM Temperaturi
                        WHERE id = %s
                        """,
                        (id,))
        except psycopg2.errors.DataError:
            cur.close()
            conn.rollback()
            app.logger.warning(id + ' not found in table')
            return {}, 404
        except psycopg2.errors.DatabaseError:
            cur.close()
            conn.rollback()
            app.logger.error('id=' + str(id) + ' could not be deleted from database')
            return {}, 400

        cur.close()
        conn.commit()

        app.logger.info('country with id=' + str(id) + ' was deleted from database')
        return {}, 200

    
    def get(self):
        global app

        parser = reqparse.RequestParser()
        parser.add_argument('lat', type=float)
        parser.add_argument('lon', type=float)
        # TODO: Check if from, until are correct timestamps?
        parser.add_argument('from', type=str)
        parser.add_argument('until', type=str)
        args = parser.parse_args()

        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        SELECT FROM Temperaturi
                        WHERE (latitudine = %(lat)s OR %(lat)s IS NULL)
                          AND (longitudine = %(lon)s OR %(lon)s IS NULL)
                          AND (timestamp >= %(from)s OR %(from)s IS NULL)
                          AND (timestamp <= %(until)s OR %(until)s IS NULL);
                        """,
                        args)
        except psycopg2.errors.DatabaseError:
            cur.close()
            app.logger.error('query with args ' + str(args) + ' failed')
            return {}, 400

        rows = cur.fetchall()
        cur.close()

        # Format response as a list of dictionaries
        cols = ['id', 'valoare', 'timestamp']
        rows = [dict(zip(cols, row)) for row in rows]

        return rows, 200


class TemperaturesCities(Resource):
    def get(self, id):
        global app

        parser = reqparse.RequestParser()
        # TODO: Check if from, until are correct timestamps?
        parser.add_argument('from', type=str)
        parser.add_argument('until', type=str)
        args = parser.parse_args()
        args['id'] = id

        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        SELECT FROM Temperaturi
                        WHERE id_oras = %(id)s
                          AND (timestamp >= %(from)s OR %(from)s IS NULL)
                          AND (timestamp <= %(until)s OR %(until)s IS NULL);
                        """,
                        args)
        except psycopg2.errors.DatabaseError:
            cur.close()
            app.logger.error('query with args ' + str(args) + ' failed')
            return {}, 400

        rows = cur.fetchall()
        cur.close()

        return rows, 200


class TemperaturesCountries(Resource):
    def get(self, id):
        global app

        parser = reqparse.RequestParser()
        # TODO: Check if from, until are correct timestamps?
        parser.add_argument('from', type=str)
        parser.add_argument('until', type=str)
        args = parser.parse_args()
        args['id'] = id

        global conn
        cur = conn.cursor()
        try:
            cur.execute("""
                        SELECT FROM Temperaturi
                        WHERE id_oras = %(id)s
                          AND (timestamp >= %(from)s OR %(from)s IS NULL)
                          AND (timestamp <= %(until)s OR %(until)s IS NULL);
                        """,
                        args)
        except psycopg2.errors.DatabaseError:
            cur.close()
            app.logger.error('query with args ' + str(args) + ' failed')
            return {}, 400

        rows = cur.fetchall()
        cur.close()

        return rows, 200

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

    api.add_resource(Countries, '/api/countries', endpoint='countries')
    api.add_resource(Countries, '/api/countries/<int:id>', endpoint='countries_id')

    api.add_resource(Cities, '/api/cities', endpoint='cities')
    api.add_resource(Cities, '/api/cities/<int:id>', endpoint='cities_id')
    api.add_resource(CitiesCountry, '/api/cities/country/<int:id>', endpoint='cities_country_id')

    api.add_resource(Temperatures, '/api/temperatures', endpoint='temperatures')
    api.add_resource(Temperatures, '/api/temperatures/<int:id>', endpoint='temperatures_id')

    api.add_resource(TemperaturesCities, '/api/temperatures/cities/<int:id>',
                     endpoint='temperatures_cities')
    api.add_resource(TemperaturesCountries, '/api/temperatures/countries/<int:id>',
                     endpoint='temperatures_countries')

    app.run(host='0.0.0.0', port=5000)

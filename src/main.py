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

        app.logger.error(str(country_id))
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


# TODO: Close database cursors and connection during shutdown
# TODO: Health check in docker compose before connection to DB

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app = Flask(__name__)
    api = Api(app)

    app.logger.info('Waiting for database start up')
    sleep(10.0) #TODO: Replace this with health check

    app.logger.info('Connecting to database')
    conn = getConnection()

    api.add_resource(Countries, '/api/countries')

    app.run(host='0.0.0.0', port=5000)

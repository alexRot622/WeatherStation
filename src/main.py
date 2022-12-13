import logging
import signal

from flask import Flask
from flask_restful import Api
import psycopg2

from Cities import Cities
from CitiesCountry import CitiesCountry
from Countries import Countries
from TemperaturesCities import TemperaturesCities
from TemperaturesCountries import TemperaturesCountries
from Temperatures import Temperatures


def handler(signum, frame):
    global app
    global conn

    if conn:
        app.logger.info("Database connection closed")
        conn.close()
    if app:
        app.logger.info("Shutting down")
    exit()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, handler)
    logging.basicConfig(level=logging.WARN)

    app = Flask(__name__)
    api = Api(app)

    app.logger.info('Connecting to database')
    conn = psycopg2.connect(
        host='postgresql',
        database='measurements',
        user='pgsql',
        password='pgsql'
    )

    kwargs = {'logger': app.logger, 'db': conn}
    api.add_resource(Countries, '/api/countries', endpoint='countries',
                     resource_class_kwargs=kwargs)
    api.add_resource(Countries, '/api/countries/<int:id>', endpoint='countries_id',
                     resource_class_kwargs=kwargs)

    api.add_resource(Cities, '/api/cities', endpoint='cities',
                     resource_class_kwargs=kwargs)
    api.add_resource(Cities, '/api/cities/<int:id>', endpoint='cities_id',
                     resource_class_kwargs=kwargs)

    api.add_resource(CitiesCountry, '/api/cities/country/<int:id>', endpoint='cities_country_id',
                     resource_class_kwargs=kwargs)

    api.add_resource(Temperatures, '/api/temperatures', endpoint='temperatures',
                     resource_class_kwargs=kwargs)
    api.add_resource(Temperatures, '/api/temperatures/<int:id>', endpoint='temperatures_id',
                     resource_class_kwargs=kwargs)

    api.add_resource(TemperaturesCities, '/api/temperatures/cities/<int:id>',
                     endpoint='temperatures_cities', resource_class_kwargs=kwargs)

    api.add_resource(TemperaturesCountries, '/api/temperatures/countries/<int:id>',
                     endpoint='temperatures_countries', resource_class_kwargs=kwargs)

    app.run(host='0.0.0.0', port=5000)

from flask import Flask
from flask_restful import Resource, Api
from time import sleep
import psycopg2

def getConnection():
    return psycopg2.connect(
        host="postgresql",
        database="measurements",
        user="pgsql",
        password="pgsql"
    )

class Countries(Resource):
    def get(self):
        global conn

        cur = conn.cursor()
        cur.execute("SELECT * FROM Tari")
        row = cur.fetchone()
        cur.close()

        return {'row': row}

# TODO: Close database cursors and connection during shutdown
# TODO: Health check in docker compose before connection to DB

if __name__ == '__main__':
    app.logger.info("Waiting for database start up")
    sleep(10.0) #TODO: Replace this with health check

    app.logger.info("Starting API")
    app = Flask(__name__)
    api = Api(app)

    app.logger.info("Connecting to database")
    conn = getConnection()

    api.add_resource(Countries, '/')

    app.run(host='0.0.0.0', port=5000)

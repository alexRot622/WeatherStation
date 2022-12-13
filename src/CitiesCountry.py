from flask_restful import Resource


class CitiesCountry(Resource):
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.db = kwargs['db']

    def get(self, id):
        cur = self.db.cursor()
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
        self.logger.info('Selected rows: ' + str(rows))

        return rows, 200

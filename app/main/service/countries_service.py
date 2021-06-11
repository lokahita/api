import uuid
import datetime

from app.main import db
from app.main.model.countries import Countries
from sqlalchemy import func
from flask.json import jsonify
from owslib.csw import CatalogueServiceWeb

def get_a_country(id):
    #query =  db.session.query(Countries.name, Countries.geom.ST_Envelope().ST_AsTEXT(), Countries.geom.ST_AsGeoJSON()).filter(Countries.id == id).first()
    query =  db.session.query(Countries.name, Countries.geom.ST_Envelope().ST_AsGeoJSON(), Countries.geom.ST_Envelope().ST_Centroid().ST_AsGeoJSON(), Countries.geom.ST_AsGeoJSON()).filter(Countries.id == id).first()
    #print(query)
    if (query):
        #print(query[1])
        response_object = {
                'status': 'ok',
                'message': {
                    'name' : query[0],
                    'bbox' : query[1],
                    'center' : query[2],
                    'geom' : query[3]
                }
        }
        return response_object, 200
    else:
        response_object = {
                'status': 'failed',
                'message': 'id not found',
            }

    return response_object, 200
    #Countries.query.filter_by(id=id).first()
    # 
def get_all():
    return Countries.query.order_by('name').all()

def get_countries_by_continent_id(id):
    return Countries.query.filter_by(continent_id=id).order_by('name').all()
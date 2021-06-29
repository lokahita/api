import uuid
import datetime

from app.main import db
from app.main.model.regions import Regions
from sqlalchemy import func
from flask.json import jsonify

def get_a_region(id):
    #query =  db.session.query(Countries.name, Countries.geom.ST_Envelope().ST_AsTEXT(), Countries.geom.ST_AsGeoJSON()).filter(Countries.id == id).first()
    query =  db.session.query(Regions.name, Regions.geom.ST_Transform(3857).ST_Envelope().ST_AsGeoJSON(), Regions.geom.ST_Envelope().ST_AsGeoJSON(), Regions.geom.ST_Transform(3857).ST_AsGeoJSON()).filter(Regions.id == id).first()
    #print(query)
    if (query):
        #print(query[1])
        response_object = {
                'status': 'ok',
                'message': {
                    'name' : query[0],
                    'bbox' : query[1],
                    'center': query[2],
                    'geom' : query[3]
                },
            }
    else:
        response_object = {
                'status': 'failed',
                'message': 'id not found',
            }

    return response_object, 200
    #Countries.query.filter_by(id=id).first()
    # 

def get_all():
    return Regions.query.order_by('name').all()

def get_regions_by_country_id(id):
    return Regions.query.filter_by(country_id=id).order_by('name').all()

def get_region_by_query(q):
    search = "%{}%".format(q)
    #print(search)
    return Regions.query.filter(Regions.name.ilike(search)).all()

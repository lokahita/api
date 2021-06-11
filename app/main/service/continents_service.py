import uuid
import datetime

from app.main import db
from app.main.model.continents import Continents
from sqlalchemy import func
from flask.json import jsonify
from owslib.csw import CatalogueServiceWeb

def get_a_continent(id):
    return Continents.query.filter_by(id=id).first()
    '''
    #query =  db.session.query(Countries.name, Countries.geom.ST_Envelope().ST_AsTEXT(), Countries.geom.ST_AsGeoJSON()).filter(Countries.id == id).first()
    query =  db.session.query(Continents.name).filter(Continents.id == id).first()
    #print(query)
    if (query):
        #print(query[1])
        response_object = {
                'status': 'ok',
                'message': {
                    'name' : query[0],
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
    '''
def get_all():
    return Continents.query.order_by('name').all()

def save_changes(data):
    db.session.add(data)
    db.session.commit()

def save_new_continent(data):
    continent = Continents.query.filter_by(name=data['name']).first()
    if not continent:
        new_continent = Continents(
            name=data['name']
        )
        save_changes(new_continent)
        response_object = {
            'status': 'success',
            'message': 'Successfully created.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Continent already exists',
        }
        return response_object, 200

def update_continent(data):
    continent = Continents.query.filter_by(id=data['id']).first()
    if continent:
        setattr(continent, 'name', data['name'])
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully updated.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Continent not found',
        }
        return response_object, 200

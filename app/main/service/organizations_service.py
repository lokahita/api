import uuid
import datetime

from app.main import db
from app.main.model.organizations import Organizations
from sqlalchemy import func
from flask.json import jsonify
from owslib.csw import CatalogueServiceWeb

def save_new_organization(data):
    organization = Organizations.query.filter_by(name=data['name']).first()
    if not organization:
        new_organization = Organizations(
            name=data['name'],
            csw=data['csw'],
        )
        save_changes(new_organization)
        response_object = {
            'status': 'success',
            'message': 'Successfully registered.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Organization already exists',
        }
        return response_object, 200

def get_an_organization(id):
    return Organizations.query.filter_by(id=id).first()
    # 
def get_all():
    return Organizations.query.order_by('name').all()

def save_changes(data):
    db.session.add(data)
    db.session.commit()

def update_organization(data):
    organization = Organizations.query.filter_by(id=data['id']).first()
    if organization:
        setattr(organization, 'name', data['name'])
        setattr(organization, 'csw', data['csw'])
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully updated.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Organization not found',
        }
        return response_object, 200

def delete_organization(data):
    organization = Organizations.query.filter_by(id=data['id']).first()
    if organization:
        Organizations.query.filter_by(id=data['id']).delete()
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully deleted.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Organization not found',
        }
        return response_object, 200

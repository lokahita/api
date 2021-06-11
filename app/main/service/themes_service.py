import uuid
import datetime

from app.main import db
from app.main.model.themes import Themes
from sqlalchemy import func
from flask.json import jsonify
from owslib.csw import CatalogueServiceWeb

def save_new_theme(data):
    theme = Themes.query.filter_by(name=data['name']).first()
    if not theme:
        new_theme = Themes(
            name=data['name']
        )
        save_changes(new_theme)
        response_object = {
            'status': 'success',
            'message': 'Successfully created.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Theme already exists',
        }
        return response_object, 200

def get_a_theme(id):
    return Themes.query.filter_by(id=id).first()
    # 
def get_all():
    return Themes.query.order_by('name').all()

def save_changes(data):
    db.session.add(data)
    db.session.commit()

def update_theme(data):
    theme = Themes.query.filter_by(id=data['id']).first()
    if theme:
        setattr(theme, 'name', data['name'])
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully updated.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Theme not found',
        }
        return response_object, 200

def delete_theme(data):
    theme = Themes.query.filter_by(id=data['id']).first()
    if theme:
        Themes.query.filter_by(id=data['id']).delete()
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully deleted.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Theme not found',
        }
        return response_object, 200

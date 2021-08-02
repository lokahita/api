import uuid
import datetime

from app.main import db
from app.main.model.keywords import Keywords
from sqlalchemy import func
from flask.json import jsonify

def save_new_keyword(data):
    row = Keywords.query.filter_by(name=data['name']).first()
    if not row:
        new_row = Keywords(
            name=data['name']
        )
        save_changes(new_row)
        response_object = {
            'status': 'success',
            'message': 'Successfully created.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Keywords already exists',
        }
        return response_object, 200

def get_a_keyword(id):
    return Keywords.query.filter_by(id=id).first()
    # 
def get_all():
    return Keywords.query.order_by('name').all()

def save_changes(data):
    db.session.add(data)
    db.session.commit()

def update_keyword(data):
    row = Keywords.query.filter_by(id=data['id']).first()
    if row:
        setattr(row, 'name', data['name'])
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

def delete_keyword(data):
    row = Keywords.query.filter_by(id=data['id']).first()
    if row:
        Keywords.query.filter_by(id=data['id']).delete()
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

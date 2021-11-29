import uuid
import datetime

from app.main import db
from app.main.model.contributions import Contributions
from app.main.model.user import User
from app.main.model.metadata import Metadata
from sqlalchemy import func
from flask.json import jsonify
import logging

import os
import configparser

from sqlalchemy import create_engine, func, __version__, select
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import create_session

import json

def save_new_contribution(data):
    print(data)
    user = User.query.filter_by(username=data['username']).first()
    #print(user.__dict__)
    if user:
        new_contribution = Contributions(
            data_name=data['name'],
            filename=data['filename'],
            time_uploaded=datetime.datetime.utcnow(),
            url=data['url'],
            user_id=user.id
        )
        save_changes(new_contribution)
        #response_object = {
        #    'status': 'success',
        #    'message': 'Successfully registered.'
        #}
        response_object = {
            'status': 'success',
            'message': 'Successfully created.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'User not found',
        }
        return response_object, 200

def get_a_contribution(id):
    return Contributions.query.filter_by(id=id).first()
    # 
def get_all():
    return Contributions.query.order_by('data_name').all()

def get_all_username(user):
    user = User.query.filter_by(username=user).first()
    #print(user.__dict__)
    if user:
        return Contributions.query.filter_by(user_id=user.id).order_by('time_uploaded').all()
    else:
        response_object = {
            'status': 'fail',
            'message': 'User not found',
        }
        return response_object, 200

def save_changes(data):
    db.session.add(data)
    db.session.commit()

def update_contribution(data):
    contribution = Contributions.query.filter_by(id=data['id']).first()
    if contribution:
        setattr(contribution, 'data_name', data['name'])
        setattr(contribution, 'filename', data['filename'])
        setattr(contribution, 'url', data['url'])
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

def delete_contribution(data):
    metadata = Contributions.query.filter_by(id=data['id']).first()
    if metadata:
        Contributions.query.filter_by(id=data['id']).delete()
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully deleted.'
        }
        return response_object, 200
    else:
        response_object = {
            'status': 'fail',
            'message': 'Data not found',
        }
        return response_object, 200

def get_count_user():
    query =  db.session.query(User.username, func.count(Metadata.user_id)).join(User).order_by(User.username).group_by(User.username).all()
    print(query)
    data = []
    for i in query: 
        #print(i.as_dict())
        print(i)
        data.append({
                    'username' : i[0],
                    'count' : i[1]
                }
        )
    print(data)
    response_object = {
            'data': data
    }
    return response_object, 200

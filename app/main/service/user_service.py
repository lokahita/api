import uuid
import datetime

from app.main import db
from app.main.model.user import User


def save_new_user(data):
    user = User.query.filter_by(email=data['email']).first()
    if not user:
        username = User.query.filter_by(username=data['username']).first()
        if not username:
            new_user = User(
                public_id=str(uuid.uuid4()),
                email=data['email'],
                fullname=data['fullname'],
                username=data['username'],
                password=data['password'],
                registered_on=datetime.datetime.utcnow()
            )
            save_changes(new_user)
            #response_object = {
            #    'status': 'success',
            #    'message': 'Successfully registered.'
            #}
            #return response_object, 201
            return generate_token(new_user)
        else:
            response_object = {
                'status': 'fail',
                'message': 'Username already exists',
            }
            return response_object, 409
    else:
        response_object = {
            'status': 'fail',
            'message': 'Email already exists',
        }
        return response_object, 409

def get_all_users():
    return User.query.filter_by(admin=False).all()

def get_a_user(public_id):
    return User.query.filter_by(public_id=public_id).first()
	
def get_a_user_email(email):
    return User.query.filter_by(email=email).first()

def get_a_user_username(username):
    return User.query.filter_by(username=username).first()

def save_changes(data):
    db.session.add(data)
    db.session.commit()

def generate_token(user):
    try:
        # generate the auth token
        auth_token = user.encode_auth_token(user.id)
        response_object = {
            'status': 'success',
            'message': 'Successfully registered.',
            'Authorization': auth_token.decode()
        }
        return response_object, 201
    except Exception as e:
        response_object = {
            'status': 'fail',
            'message': 'Some error occurred. Please try again.'
        }
        return response_object, 401


def delete_user(data):
    user = User.query.filter_by(public_id=data['public_id']).first()
    if user:
        User.query.filter_by(public_id=data['public_id']).delete()
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully deleted.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'User not found',
        }
        return response_object, 200


def update_user(data):
    user = User.query.filter_by(public_id=data['public_id']).first()
    if user:
        setattr(user, 'fullname', data['fullname'])
        setattr(user, 'email', data['email'])
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully updated.'
        }
        return response_object, 200
    else:
        response_object = {
            'status': 'fail',
            'message': 'User not found',
        }
        return response_object, 200

def password_user(data):
    user = User.query.filter_by(public_id=data['public_id']).first()

    if user:
        if user.check_password(data['current_password']):
            if (data['new_password'] == data['repeat_password']):
                setattr(user, 'password', data['new_password'])
                db.session.commit()
                response_object = {
                    'status': 'success',
                    'message': 'Successfully updated.'
                }
                return response_object, 200
            else:
                response_object = {
                    'status': 'fail',
                    'message': 'New password and repeat password do not match',
                }
                return response_object, 200
        else:
            response_object = {
                'status': 'fail',
                'message': 'Current password is incorrect',
            }
            return response_object, 200
    else:
        response_object = {
            'status': 'fail',
            'message': 'User not found',
        }
        return response_object, 200
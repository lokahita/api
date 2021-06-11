from app.main.model.user import User
from ..service.blacklist_service import save_token


class Auth:

    @staticmethod
    def login_user(data):
        try:
            # fetch the user data
            user = User.query.filter_by(username=data.get('username')).first()
            print(user)
            if user and user.check_password(data.get('password')):
                auth_token = user.encode_auth_token(user.id)
                print(auth_token)
                if auth_token:
                    response_object = {
                        'status': 'success',
                        'message': 'Successfully logged in.',
                        'Authorization': auth_token.decode(),
						'username': user.username,
						'public_id': user.public_id
                    }
                    return response_object, 200
            else:
                response_object = {
                    'status': 'fail',
                    'message': 'username or password does not match.'
                }
                return response_object, 401

        except Exception as e:
            print(e)
            response_object = {
                'status': 'fail',
                'message': 'Try again'
            }
            return response_object, 500
    
    @staticmethod
    def login_admin(data):
        try:
            # fetch the user data
            user = User.query.filter_by(username=data.get('username'), admin=True).first()
            print(user)
            if user:
                if user.check_password(data.get('password')):
                    auth_token = user.encode_auth_token(user.id)
                    print(auth_token)
                    if auth_token:
                        response_object = {
                            'status': 'success',
                            'message': 'Successfully logged in.',
                            'Authorization': auth_token.decode(),
                            'username': user.username,
                            'public_id': user.public_id
                        }
                        return response_object, 200
                else:
                    response_object = {
                        'status': 'fail',
                        'message': 'password does not match.'
                    }
                    return response_object, 401
            else:
                response_object = {
                    'status': 'fail',
                    'message': 'username is not admin.'
                }
                return response_object, 401

        except Exception as e:
            print(e)
            response_object = {
                'status': 'fail',
                'message': 'Try again'
            }
            return response_object, 500

    @staticmethod
    def logout_user(data):
        #print(data)
        if data:
            auth_token = data #data.split(" ")[1]
        else:
            auth_token = ''
        print(auth_token + " --- ")
        if auth_token:
            resp = User.decode_auth_token(auth_token)
            if not isinstance(resp, str): #not
                # mark the token as blacklisted
                return save_token(token=auth_token)
            else:
                response_object = {
                    'status': 'fail',
                    'message': resp
                }
                return response_object, 401
        else:
            response_object = {
                'status': 'fail',
                'message': 'Provide a valid auth token.'
            }
            return response_object, 403

    @staticmethod
    def get_logged_in_user(new_request):
        # get the auth token
        #print(new_request.headers)
        auth_token = new_request.headers.get('Authorization')
        #print(auth_token)
        if auth_token:
            resp = User.decode_auth_token(auth_token)
            #print(resp)
            #print(type(resp))
            if not isinstance(resp, str):
                user = User.query.filter_by(id=resp).first()
                response_object = {
                    'status': 'success',
                    'data': {
                        'user_id': user.public_id,
                        'email': user.email,
                        'admin': user.admin
                        #'registered_on': str(user.registered_on)
                    }
                }
                return response_object, 200
            response_object = {
                'status': 'fail',
                'message': resp
            }
            return response_object, 401
        else:
            response_object = {
                'status': 'fail',
                'message': 'Provide a valid auth token.'
            }
            return response_object, 401
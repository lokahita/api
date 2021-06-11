from flask import request
from flask_restplus import Resource

from app.main.service.auth_helper import Auth
from ..util.dto import AuthDto
from ..util.decorator import admin_token_required, token_required

api = AuthDto.api
user_auth = AuthDto.user_auth

@api.route('/login')
class UserLogin(Resource):
    """
        User Login Resource
    """
    @api.doc('user login')
    @api.expect(user_auth, validate=True)
    def post(self):
        # get the post data
        #print(request.json)
        post_data = request.json
        return Auth.login_user(data=post_data)

@api.route('/loginAdmin')
class UserLoginAdmin(Resource):
    """
        Admin Login Resource
    """
    @api.doc('admin login')
    @api.expect(user_auth, validate=True)
    def post(self):
        # get the post data
        #print(request.json)
        post_data = request.json
        return Auth.login_admin(data=post_data)

@api.route('/validUser')
#@api.header('Authorization: Bearer', 'JWT TOKEN', required=True)
class ValidUser(Resource):
    @api.response(401, 'Unauthorized.')
    @api.doc('valid User')
    @token_required
    def get(self):
        """Check Valid User"""
        response_object = {
                'status': 'success',
                'message': 'User Valid',
        }
        return response_object, 200

@api.route('/validAdmin')
#@api.header('Authorization: Bearer', 'JWT TOKEN', required=True)
class ValidAdmin(Resource):
    @api.response(401, 'Unauthorized.')
    @api.doc('valid Admin')
    @admin_token_required
    def get(self):
        """Check Valid Admin"""
        response_object = {
                'status': 'success',
                'message': 'Admin Valid',
        }
        return response_object, 200

@api.route('/logout')
class LogoutAPI(Resource):
    """
    Logout Resource
    """
    @api.doc('logout a user')
    def post(self):
        # get auth token
        auth_header = request.headers.get('Authorization')
        return Auth.logout_user(data=auth_header)
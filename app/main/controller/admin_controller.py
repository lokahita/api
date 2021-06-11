from flask import request
from flask_restplus import Resource

from ..util.dto import AkunDto
from ..service.admin_service import save_new_token, validitas_akun, logout_akun

api = AdminDto.api
_akun_auth = AdminDto.akun_auth


@api.route('/login')
class Akun(Resource):
    """
        Login Resource
    """
    @api.doc('check login and create a new token')
    @api.expect(_akun_auth, validate=True)
    def post(self):
        data = request.json
        return save_new_token(data=data)

@api.route('/valid')
class ValidAkun(Resource):
    """
    Valid Resource
    """
    @api.doc('validity akun')
    def post(self):
        # get auth token
        auth_header = request.headers.get('Authorization')
        return validitas_akun(data=auth_header)

@api.route('/logout')
class LogoutAkun(Resource):
    """
    Logout Resource
    """
    @api.doc('logout akun')
    def post(self):
        # get auth token
        auth_header = request.headers.get('Authorization')
        return logout_akun(data=auth_header)
from flask import request, send_from_directory
from flask_restplus import Resource
#import pysftp as sftp
import os
from ..util.dto import QueryDto
# from ..util.decorator import token_required
from ..service.query_service import list_year

api = QueryDto.api

@api.route('/year/')
class QueryDtoList(Resource):
    @api.doc('list year in harvested')
    #@api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all organizations"""
        return list_year()
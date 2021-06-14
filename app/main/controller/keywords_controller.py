from flask import request
from flask_restplus import Resource

from ..util.dto import KeywordsDto
from ..service.keywords_service import get_a_keyword, get_all, save_new_keyword, update_keyword

api = KeywordsDto.api
_schema =  KeywordsDto.schema
_entry =  KeywordsDto.entry
_update =  KeywordsDto.update

@api.route('/')
class KeywordsDtoList(Resource):
    @api.doc('list of keywords')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all keywords"""
        return get_all()

    @api.response(201, 'Keywords successfully created.')
    @api.doc('create a new keywords')
    @api.expect(_entry, validate=True)
    def post(self):
        """Creates a new Keywords """
        data = request.json

        return save_new_keyword(data=data)

@api.route('/id/<int:id>')
@api.param('id', 'The Keyword id')
@api.response(404, 'Keyword not found.')
class Keywords(Resource):
    @api.doc('get a keyword')
    @api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get a keyword given its id"""
        row = get_a_keyword(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Keyword ID is not found.',
        	}
        	return response_object, 404
        else:
            return row

@api.route('/update/')
class KeywordUpdate(Resource):
    @api.response(201, 'Keyword successfully updated.')
    @api.doc('update a keyword')
    @api.expect(_update, validate=True)
    def post(self):
        """Update a keyword"""
        data = request.json
        return update_keyword(data=data)
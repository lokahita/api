from flask import request
from flask_restplus import Resource

from ..util.dto import ContinentsDto
from ..service.continents_service import get_a_continent, get_all, save_new_continent, update_continent

api = ContinentsDto.api
_schema =  ContinentsDto.schema
#_entry =  ContinentsDto.entry

@api.route('/')
class ContinentsList(Resource):
    @api.doc('list of continents')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all types"""
        return get_all()
    '''
    @api.response(201, 'Continent successfully created.')
    @api.doc('create a new continent')
    @api.expect(_entry, validate=True)
    def post(self):
        """Creates a new Continent """
        data = request.json
        return save_new_continent(data=data)
    '''
@api.route('/id/<int:id>')
@api.param('id', 'The Continent id')
@api.response(404, 'Continent not found.')
class Continents(Resource):
    @api.doc('get a continent')
    @api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get a Continent given its id"""
        row = get_a_continent(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Continent ID is not found.',
        	}
        	return response_object, 404
        else:
            return row
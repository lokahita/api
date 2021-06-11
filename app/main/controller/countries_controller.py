from flask import request
from flask_restplus import Resource

from ..util.dto import CountriesDto
from ..service.countries_service import get_a_country, get_all, get_countries_by_continent_id

api = CountriesDto.api
_schema =  CountriesDto.schema

@api.route('/')
class CountryList(Resource):
    @api.doc('list of countries')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all types"""
        return get_all()

@api.route('/continent/<int:id>')
@api.param('id', 'The Continent id')
@api.response(404, 'Country not found.')
class CountriesContinentList(Resource):
    @api.doc('list of countries by continent id')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self, id):
        """List all countries by continent id"""
        return get_countries_by_continent_id(id)

@api.route('/id/<int:id>')
@api.param('id', 'The Country id')
@api.response(404, 'Country not found.')
class Countries(Resource):
    @api.doc('get a country')
    #@api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get a Country given its id"""
        row = get_a_country(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Country ID is not found.',
        	}
        	return response_object, 404
        else:
            return row
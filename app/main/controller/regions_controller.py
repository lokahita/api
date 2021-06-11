from flask import request
from flask_restplus import Resource

from ..util.dto import RegionsDto
from ..service.regions_service import get_a_region, get_all, get_regions_by_country_id, get_region_by_query

api = RegionsDto.api
_schema =  RegionsDto.schema
_schema_country  =  RegionsDto.schema_country

@api.route('/')
class RegionsList(Resource):
    @api.doc('list of regions')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all types"""
        return get_all()

@api.route('/country/<int:id>')
@api.param('id', 'The Country id')
@api.response(404, 'Region not found.')
class RegionsCountryList(Resource):
    @api.doc('list of regions by country id')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self, id):
        """List all types"""
        return get_regions_by_country_id(id)

@api.route('/search/<query>')
@api.param('query', 'The query')
@api.response(404, 'Region not found.')
class RegionsQueryList(Resource):
    @api.doc('list of regions by query')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self, query):
        """List all types"""
        return get_region_by_query(query)

@api.route('/id/<int:id>')
@api.param('id', 'The region id')
@api.response(404, 'Region not found.')
class Regions(Resource):
    @api.doc('get a region')
    #@api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get a region given its id"""
        row = get_a_region(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Region ID is not found.',
        	}
        	return response_object, 404
        else:
            return row
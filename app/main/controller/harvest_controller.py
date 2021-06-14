from flask import request, send_from_directory
from flask_restplus import Resource
#import pysftp as sftp
import os
from ..util.dto import HarvestingsDto
# from ..util.decorator import token_required
from ..service.harvest_service import harvest_an_organization, get_all, get_harvest_by_organization_id, get_harvest_by_bbox, get_harvest_by_identifier, delete_harvested, get_count_all, get_count_organization, get_count_year, get_limit, get_latest, get_alphabet, get_bbox_by_identifier

api = HarvestingsDto.api
_schema = HarvestingsDto.schema
_schema2 = HarvestingsDto.schema2
_delete =  HarvestingsDto.delete

@api.route('/')
class HarvestingsDtoList(Resource):
    @api.doc('list of harvestings')
    @api.marshal_list_with(_schema2, envelope='data')
    #@admin_token_required
    def get(self):
        """List all harvestings"""
        return get_all()

@api.route('/pull/<int:id>')
@api.param('id', 'id organization to be harvested')
# @api.response(404, 'User not found.')
class HarvestPulls(Resource):
    @api.doc('harvest an organization')
    # @api.marshal_with(_download)
    # @token_required
    def get(self, id):
        """harvest an organization given its identifier"""
        return harvest_an_organization(id) 

@api.route('/organization/<int:id>')
@api.param('id', 'id organization')
# @api.response(404, 'User not found.')
class Harvest(Resource):
    @api.doc('list of harvestings by organization id')
    @api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self, id):
        """list of harvestings by organization id"""
        return get_harvest_by_organization_id(id) 

@api.route('/bbox/<float(signed=True):xmin>/<float(signed=True):ymin>/<float(signed=True):xmax>/<float(signed=True):ymax>')
@api.param('xmin', 'xmin')
@api.param('ymin', 'ymin')
@api.param('xmax', 'xmax')
@api.param('ymax', 'ymax')
# @api.response(404, 'User not found.')
class HarvestBbox(Resource):
    @api.doc('list of harvestings by bbox')
    @api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self, xmin, ymin, xmax, ymax):
        """list of harvestings by bbox"""
        return get_harvest_by_bbox(xmin, ymin, xmax, ymax) 



@api.route('/bbox/<float(signed=True):xmin>/<float(signed=True):ymin>/<float(signed=True):xmax>/<float(signed=True):ymax>')
@api.param('xmin', 'xmin')
@api.param('ymin', 'ymin')
@api.param('xmax', 'xmax')
@api.param('ymax', 'ymax')
# @api.response(404, 'User not found.')
class HarvestBboxTheme(Resource):
    @api.doc('list of harvestings by bbox')
    @api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self, xmin, ymin, xmax, ymax):
        """list of harvestings by bbox"""
        return get_harvest_by_bbox(xmin, ymin, xmax, ymax) 


@api.route('/identifier/<string:id>')
@api.param('id', 'identifier')
# @api.response(404, 'User not found.')
class HarvestIdentifier(Resource):
    @api.doc('list of harvestings by identifier')
    @api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self, id):
        """list of harvestings by identifier id"""
        return get_harvest_by_identifier(id) 

@api.route('/bbox_identifier/<string:id>')
@api.param('id', 'identifier')
# @api.response(404, 'User not found.')
class HarvestBboxIdentifier(Resource):
    @api.doc('list of harvestings by identifier')
    #@api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self, id):
        """list of harvestings by identifier id"""
        return get_bbox_by_identifier(id) 

@api.route('/delete/')
class HarvestDelete(Resource):
    @api.response(201, 'Harvest successfully deleted.')
    @api.doc('delete a harvested data')
    @api.expect(_delete, validate=True)
    #@admin_token_required
    def post(self):
        """Delete a harvested data"""
        data = request.json
        return delete_harvested(data=data)

@api.route('/count/')
class HarvestCount(Resource):
    @api.response(200, 'Harvest successfully counted.')
    @api.doc('count total harvested data')
    #@api.expect(_delete, validate=True)
    #@admin_token_required
    def get(self):
        """Count total harvested data"""
        return get_count_all()

@api.route('/count/organization/')
class HarvestCountOrganization(Resource):
    @api.response(200, 'Harvest successfully counted.')
    @api.doc('count total harvested data by organization')
    #@api.expect(_delete, validate=True)
    #@admin_token_required
    def get(self):
        """count total harvested data by organization"""
        return get_count_organization()

@api.route('/count/year/')
class HarvestCountYear(Resource):
    @api.response(200, 'Harvest successfully counted.')
    @api.doc('count total harvested data by year')
    #@api.expect(_delete, validate=True)
    #@admin_token_required
    def get(self):
        """count total harvested data by year"""
        return get_count_year()


@api.route('/limit/<string:order>/<int:page>/<int:page_size>')
@api.param('order', 'field to be ordered')
@api.param('page', 'page number')
@api.param('page_size', 'number of record per page')
# @api.response(404, 'User not found.')
class HarvestLimitPulls(Resource):
    @api.doc('harvest an organization')
    @api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self, order, page, page_size):
        """harvest an organization given its identifier"""
        return get_limit(order, page, page_size)

@api.route('/latest/')
# @api.response(404, 'User not found.')
class HarvestLatest(Resource):
    @api.doc('harvest an organization')
    @api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self):
        """harvest an organization given its identifier"""
        return get_latest()

@api.route('/alphabet/')
# @api.response(404, 'User not found.')
class HarvestAlphabet(Resource):
    @api.doc('harvest an organization')
    @api.marshal_list_with(_schema2, envelope='data')
    # @token_required
    def get(self):
        """harvest an organization given its identifier"""
        return get_alphabet()
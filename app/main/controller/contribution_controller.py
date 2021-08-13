from flask import request
from flask_restplus import Resource
import os
from ..util.dto import ContributionDto
from ..service.contribution_service import get_a_contribution, get_all, get_all_username, save_new_contribution, update_contribution, delete_contribution, get_count_user
from ..util.decorator import admin_token_required, token_required

api = ContributionDto.api
_schema =  ContributionDto.schema
_entry =  ContributionDto.entry
_update =  ContributionDto.update
_delete =  ContributionDto.delete

#APP_ROOT = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'metadata/uploads')


@api.route('/')
class ContributionDtoList(Resource):
    @api.doc('list of contribution')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all contribution"""
        return get_all()
       
    @api.response(201, 'Contribution successfully created.')
    @api.doc('create a new contribution')
    @api.expect(_entry, validate=True)
    @token_required
    def post(self):
        """Creates a new contribution """
        data = request.json
        return save_new_contribution(data=data)

@api.route('/id/<int:id>')
@api.param('id', 'The Contribution id')
@api.response(404, 'Contribution not found.')
class Contribution(Resource):
    @api.doc('get a contribution')
    @api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get a contribution given its id"""
        row = get_a_contribution(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Contribution ID is not found.',
        	}
        	return response_object, 404
        else:
            return row

@api.route('/username/<user>')
@api.param('user', 'The Contribution username')
@api.response(404, 'Contribution not found.')
class Contribution(Resource):
    @api.doc('get a contribution')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self, user):
        """get a contribution given its username"""
        return get_all_username(user)

@api.route('/update/')
class ContributionUpdate(Resource):
    @api.response(201, 'Contribution successfully updated.')
    @api.doc('update a contribution')
    @api.expect(_update, validate=True)
    @token_required
    def post(self):
        """Update a Contribution"""
        data = request.json
        return update_contribution(data=data)

@api.route('/delete/')
class ContributionDelete(Resource):
    @api.response(201, 'Contribution successfully deleted.')
    @api.doc('delete a contribution')
    @api.expect(_delete, validate=True)
    @token_required
    def post(self):
        """Delete a Contribution """
        data = request.json
        return delete_contribution(data=data)

@api.route('/count/user/')
class ContributionUser(Resource):
    @api.response(200, 'Harvest successfully counted.')
    @api.doc('count total contribution by user')
    #@api.expect(_delete, validate=True)
    #@admin_token_required
    def get(self):
        """count total contribution by user"""
        return get_count_user()
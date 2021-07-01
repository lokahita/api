from flask import request
from flask_restplus import Resource

from ..util.dto import OrganizationsDto
from ..service.organizations_service import get_an_organization, get_all, save_new_organization, update_organization, delete_organization
from ..util.decorator import admin_token_required

api = OrganizationsDto.api
_schema =  OrganizationsDto.schema
_entry =  OrganizationsDto.entry
_update =  OrganizationsDto.update
_delete =  OrganizationsDto.delete

@api.route('/')
class OrganizationsDtoList(Resource):
    @api.doc('list of organizations')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all organizations"""
        return get_all()

    @api.response(201, 'Organization successfully created.')
    @api.doc('create a new organization')
    @api.expect(_entry, validate=True)
    @admin_token_required
    def post(self):
        """Creates a new Organization """
        data = request.json
        return save_new_organization(data=data)

@api.route('/id/<int:id>')
@api.param('id', 'The Organization id')
@api.response(404, 'Organization not found.')
class Organizations(Resource):
    @api.doc('get an organization')
    @api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get an organization given its id"""
        row = get_an_organization(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Organization ID is not found.',
        	}
        	return response_object, 404
        else:
            return row

@api.route('/update/')
class OrganizationsUpdate(Resource):
    @api.response(201, 'Organization successfully updated.')
    @api.doc('update an organization')
    @api.expect(_update, validate=True)
    @admin_token_required
    def post(self):
        """Update an Organization """
        data = request.json
        return update_organization(data=data)

@api.route('/delete/')
class OrganizationsDelete(Resource):
    @api.response(201, 'Organization successfully deleted.')
    @api.doc('delete an organization')
    @api.expect(_delete, validate=True)
    @admin_token_required
    def post(self):
        """Delete an Organization """
        data = request.json
        return delete_organization(data=data)
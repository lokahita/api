from flask import request
from flask_restplus import Resource

from ..util.dto import ThemesDto
from ..service.themes_service import get_a_theme, get_all, save_new_theme, update_theme
from ..util.decorator import admin_token_required
api = ThemesDto.api
_schema =  ThemesDto.schema
_entry =  ThemesDto.entry
_update =  ThemesDto.update

@api.route('/')
class ThemesDtoList(Resource):
    @api.doc('list of themes')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all themes"""
        return get_all()

    @api.response(201, 'Theme successfully created.')
    @api.doc('create a new theme')
    @api.expect(_entry, validate=True)
    @admin_token_required
    def post(self):
        """Creates a new Theme """
        data = request.json

        return save_new_theme(data=data)

@api.route('/id/<int:id>')
@api.param('id', 'The Theme id')
@api.response(404, 'Theme not found.')
class Themes(Resource):
    @api.doc('get an theme')
    @api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get a theme given its id"""
        row = get_a_theme(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Theme ID is not found.',
        	}
        	return response_object, 404
        else:
            return row

@api.route('/update/')
class ThemesUpdate(Resource):
    @api.response(201, 'Theme successfully updated.')
    @api.doc('update a theme')
    @api.expect(_update, validate=True)
    @admin_token_required
    def post(self):
        """Update a theme """
        data = request.json
        return update_theme(data=data)
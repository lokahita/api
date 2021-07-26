from flask import request
from flask_restplus import Resource
import os
from ..util.dto import MetadataDto
from ..service.metadata_service import get_a_metadata, get_all, save_new_metadata, update_metadata, delete_metadata
from ..util.decorator import admin_token_required, token_required

api = MetadataDto.api
_schema =  MetadataDto.schema
_entry =  MetadataDto.entry
_update =  MetadataDto.update
_delete =  MetadataDto.delete

#APP_ROOT = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'metadata/uploads')


@api.route('/')
class MetadataDtoList(Resource):
    @api.doc('list of metadata')
    @api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all metadata"""
        return get_all()

    @api.response(201, 'Metadata successfully created.')
    @api.doc('create a new metadata')
    #@api.expect(_entry, validate=True)
    #@admin_token_required
    @token_required
    def post(self):
        """Creates a new Metadata """
        #data = request.json

        file = request.files['file']
        #username= request['username']
        print(file)
        #print(request.__dict__)
        #print(request.form['username'])
        username = request.form['username']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            response_object = {
            'status': 'error',
            'message': 'Select file first'
            }
            return response_object, 200
        else:
            file.save(os.path.join(UPLOAD_FOLDER, file.filename))
            return save_new_metadata(file.filename, username)

@api.route('/id/<int:id>')
@api.param('id', 'The Metadata id')
@api.response(404, 'Metadata not found.')
class Metadata(Resource):
    @api.doc('get a metadata')
    @api.marshal_with(_schema)
    #@admin_token_required
    def get(self, id):
        """get an metadata given its id"""
        row = get_a_metadata(id)
        if not row:
        	response_object = {
        		'status': 'fail',
        		'message': 'Metadata ID is not found.',
        	}
        	return response_object, 404
        else:
            return row

@api.route('/update/')
class MetadataUpdate(Resource):
    @api.response(201, 'Metadata successfully updated.')
    @api.doc('update a metadata')
    @api.expect(_update, validate=True)
    @token_required
    def post(self):
        """Update a Metadata"""
        data = request.json
        return update_metadata(data=data)

@api.route('/delete/')
class MetadataDelete(Resource):
    @api.response(201, 'Metadata successfully deleted.')
    @api.doc('delete an metadata')
    @api.expect(_delete, validate=True)
    @token_required
    def post(self):
        """Delete an Metadata """
        data = request.json
        return delete_metadata(data=data)
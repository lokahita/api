from flask import request
from flask_restplus import Resource
import os
import subprocess 
from ..util.dto import DataDto
#from ..service.metadata_service import get_a_metadata, get_all, save_new_metadata, update_metadata, delete_metadata
from ..util.decorator import admin_token_required, token_required
import requests

api = DataDto.api
#_schema =  DataDto.schema
#_entry =  DataDto.entry
#_update =  DataDto.update
#_delete =  DataDto.delete

#APP_ROOT = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'data', 'uploads')
GEOSERVER = "http://localhost/geoserver/rest/" #"https://data.cifor.org/geoportal/geoserver/rest/"
AUTH = "Basic YWRtaW46Z2Vvc2VydmVy" #'Authorization: Basic YWRtaW46bXlhd2Vzb21lZ2Vvc2VydmVy' 
WS = 'fta'
CONTENT = "Content-type: application/zip"

@api.route('/')
class DataDtoList(Resource):
    #@api.doc('list of metadata')
    #@api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    #def get(self):
    #    """List all metadata"""
    #    return get_all()

    @api.response(201, 'Data successfully uploaded.')
    @api.doc('upload a new data')
    #@api.expect(_entry, validate=True)
    #@admin_token_required
    @token_required
    def post(self):
        """Uploads a new Data """
        #data = request.json

        file = request.files['file']
        #username= request['username']
        print(file)
        #print(request.__dict__)
        #print(request.form['username'])
        username = request.form['username']
        #shapeFile = request.form['shapeFile']
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
            return publish_geoserver(file.filename, username)


def publish_geoserver(filename, username):
    print(filename, username)
    result = subprocess.run([
    'curl', 
    '-v', 
    '-XPUT', 
    '-H',
    AUTH,
    '-H',
    CONTENT,
    '--data-binary',
    '@'+os.path.join(UPLOAD_FOLDER,filename),
    GEOSERVER + 'workspaces/'+WS+'/datastores/'+ username+'/file.shp'
    ], stdout=subprocess.PIPE)
    print(result)
    print(result.stdout.decode('utf-8'))
    #url_store = GEOSERVER + 'workspaces/'+WS+'/datastores/' + username + '.json'
    response_object = {
                        'status': 'success',
                        'message': 'Successfully uploaded'
                    }
    return response_object, 201

    

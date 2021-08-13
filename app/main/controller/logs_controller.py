from flask import request, json, send_from_directory
from flask_restplus import Resource
import os
from ..util.dto import LogsDto
from ..util.decorator import admin_token_required, token_required
from os import listdir
from os.path import isfile, join

api = LogsDto.api
#_schema =  ContributionDto.schema
#_entry =  ContributionDto.entry
#_update =  ContributionDto.update
#_delete =  ContributionDto.delete

#APP_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_FOLDER = os.path.join(os.getcwd(), 'data/logs')


@api.route('/')
class LogsList(Resource):
    @api.doc('list of logs')
    #@api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self):
        """List all contribution"""
        onlyfiles = [f for f in listdir(LOG_FOLDER) if isfile(join(LOG_FOLDER, f))]
        results =  []
        for i in onlyfiles: 
            #print(i.as_dict())
            print(i)
            sp = i.split("_")
            results.append({
                        'file' : i,
                        'organization' : sp[0],
                        'time' : sp[1]+"_"+sp[2],
                        'total' : sp[3].replace(".txt", "")
                    }
            )
        print(results)
        result_dict = {'data': results}
        #response_object = {
        #        'status': 'oke',
        #        'data': jsonify([dict(row) for row in result])
        #}
        return result_dict, 200
        #return json.dumps(onlyfiles)
        #return get_all()

@api.route('/download/<file>')
@api.param('file', 'The Log file')
@api.response(404, 'Log not found.')
class Logs(Resource):
    @api.doc('get a log')
    #@api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self, file):
        """get a log given its filename"""
        try:
            """Download a file."""

            return send_from_directory(LOG_FOLDER, file, as_attachment=True)
            #path = os.join.pa"D:\DEMNAS_PAK_IBNU\demnas_api\download\" + file_name
            #return send_file(path, as_attachment=True)
            #return send_from_directory(directory='pdf', filename=filename)
            #s = sftp.Connection(host='192.168.210.181', username='anambas', password='4n4mb4s01')

            #remote_path = '/home/anambas/download/DEMNAS_0911-32_v1.0.tif'
            #local_path = '/Users/PPIG/python/demnas/download/DEMNAS_0911-32_v1.0.tif'

            #s.get(remote_path, local_path)

            #s.close
        except Exception as e:
            print(e)

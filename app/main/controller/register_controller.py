from flask import request, send_from_directory
from flask_restplus import Resource
#import pysftp as sftp
import os
from ..util.dto import RegisterDto
# from ..util.decorator import token_required
# from ..service.user_service import save_new_user, get_all_users, get_a_user

api = RegisterDto.api
# _download = DownloadDto.download
FOLDER_DIRECTORY = 'D:\DEMNAS_PAK_IBNU\demnas_api\download'

@api.route('/<file_name>')
@api.param('file_name', 'name of file')
# @api.response(404, 'User not found.')
class Demnas(Resource):
    @api.doc('get a file')
    # @api.marshal_with(_download)
    # @token_required
    def get(self, file_name):
        """get a user given its identifier"""
        try:
            """Download a file."""

            return send_from_directory(FOLDER_DIRECTORY, file_name, as_attachment=True)
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
from flask import request
from flask_restplus import Resource
from ..model.m_statistic import Statistic, StatisticSchema
from marshmallow import ValidationError

from ..util.dto import StatisticDto

api = StatisticDto.api
_statistic = StatisticDto.statistic
statistic_schema = StatisticSchema()

@api.route('/')
class StatisticList(Resource):
    @api.doc('list_of_statistic')
    @api.marshal_list_with(_statistic, envelope='data')
    def get(self):
        """List all tatistic """
        return Statistic.get_all_statistic()

    @api.response(200, 'Statistic successfully created.')
    @api.doc('create a new statistic')
    @api.expect(_statistic, validate=True)
    def post(self):
        """Creates a new Statistic """
        req_data = request.json
        try:
            data = statistic_schema.load(req_data)
            statistic = Statistic(data)
            statistic.save()
            return statistic_schema.dump(statistic)
        except ValidationError as err:
            return err.messages  

@api.route('/<id>', methods=['GET', 'PUT', 'DELETE'])
@api.param('id', 'The Statistic identifier')
@api.response(404, 'Statistic not found.')
class StatisticById(Resource):
    @api.doc('get a statistic')
    @api.marshal_with(_statistic)
    def get(self, id):
        """Get a statistic given its identifier"""
        statistic = Statistic.get_statistic_by_id(id)
        if not statistic:
            api.abort(404)
        else:
            return statistic

@api.route('/delete/<id>', methods=['POST'])
@api.param('id', 'The Statistic identifier')
@api.response(404, 'Statistic not found.')
class StatisticDelete(Resource):     
    def post(self, id):
        """Delete an statistic"""
        try:
            statistic = Statistic.get_statistic_by_id(id)
            statistic.delete()
            response_object = {
                'status': 'Success',
                'message': 'Statistic already deleted',
            }
            return response_object, 200
        except:
            response_object = {
                'status': 'Failed',
                'message': 'Statistic not found',
            }
            return response_object, 404
from flask_restplus import Namespace, fields

class AkunDto:
    api = Namespace('akun', description='akun related operations')
    akun_auth = api.model('akun_details', {
        'username': fields.String(required=True, description='The username'),
        'password': fields.String(required=True, description='The password '),
    })

class DownloadDto:
    api = Namespace('download', description='download related operations')
    # download = api.model('user', {
    #     'filename': fields.String(required=True, description='name of file'),
    # })

class StatisticDto:
    api = Namespace('statistic', description='statistic download demnas')
    statistic = api.model('Statistic',{
        # 'id': fields.Integer(required=True, description='id auto increment'),
        'username': fields.String(required=True, description='username'),
        'created_date': fields.DateTime(description='datetime data created'),
        'file_name': fields.String(required=True, description='file name'),
    })

class RegisterDto:
    api = Namespace('register', description='register related operations')
    # download = api.model('user', {
    #     'filename': fields.String(required=True, description='name of file'),
    # })

class QueryDto:
    api = Namespace('query', description='query related operations')
    # download = api.model('user', {
    #     'filename': fields.String(required=True, description='name of file'),
    # })

class ContinentsDto:
    api = Namespace('continents', description='continents related operations')
    schema = api.model('continents', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='continents name')
    })

class CountriesDto:
    api = Namespace('countries', description='countries related operations')
    schema = api.model('countries', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='country name')
    })
    schema_continent = api.model('countries', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='region name'),
        'continent' : fields.Nested(ContinentsDto().schema, only=['id', 'name'], required=True)
    })


class RegionsDto:
    api = Namespace('regions', description='regions related operations')
    schema = api.model('regions', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='region name')
    })
    
    schema_country = api.model('regions', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='region name'),
        'country' : fields.Nested(CountriesDto().schema, only=['id', 'name'], required=True)
    })

class OrganizationsDto:
    api = Namespace('organizations', description='organizations related operations')
    schema = api.model('organizations', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='organization name'),
        'csw': fields.String(required=True, description='csw url'),
    })
    schema2 = api.model('organizations', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='organization name')
    })
    entry = api.model('organizations', {
        'name': fields.String(required=True, description='organization name'),
        'csw': fields.String(required=True, description='csw url'),
    })
    update = api.model('organizations', {
        'id': fields.Integer(required=True, description='id'),
        'name': fields.String(required=True, description='organization name'),
        'csw': fields.String(required=True, description='csw url'),
    })
    delete = api.model('organizations', {
        'id': fields.Integer(required=True, description='id'),
    })

class MetadataDto:
    api = Namespace('metadata', description='metadata related operations')
    schema = api.model('metadata', {
        'id': fields.Integer(dump_only=True),
        'filename': fields.String(required=True, description='metadata name'),
        'time_uploaded': fields.DateTime(description='datetime metadata uploaded'),
    })
    schema2 = api.model('metadata', {
        'id': fields.Integer(dump_only=True),
        'filename': fields.String(required=True, description='metadata name')
    })
    entry = api.model('metadata', {
        'filename': fields.String(required=True, description='metadata name'),
    })
    update = api.model('metadata', {
        'id': fields.Integer(required=True, description='id'),
        'filename': fields.String(required=True, description='metadata name'),
    })
    delete = api.model('metadata', {
        'id': fields.Integer(required=True, description='id'),
    })

class HarvestingsDto:
    api = Namespace('harvestings', description='harvestings related operations')
    schema = api.model('harvestings', {
        'id': fields.Integer(dump_only=True),
        'title': fields.String(required=True, description='title name'),
        'abstract': fields.String(required=True, description='abstract'),
        'organizations' : fields.Nested(OrganizationsDto().schema2, only=['id', 'name'], required=True)
    })
    schema2 = api.model('harvestings', {
        'id': fields.Integer(dump_only=True),
        'title': fields.String(required=True, description='title name'),
        'abstract': fields.String(required=True, description='abstract'),
        'organizations' : fields.Nested(OrganizationsDto().schema2, only=['id', 'name'], required=True),
        'data_type': fields.String(required=True, description='data type'),
        'identifier': fields.String(required=True, description='identifier'),
        'modified': fields.DateTime(required=True, description='data type'),
        'references': fields.String(required=True, description='data type'),
        'subjects': fields.String(required=True, description='subjects')
    })
    delete = api.model('metadata', {
        'id': fields.Integer(required=True, description='id'),
    })
    #bbox = db.Column(Geometry('POLYGON', srid='4326'))

class ThemesDto:
    api = Namespace('themes', description='themes related operations')
    schema = api.model('themes', {
        'id': fields.Integer(dump_only=True),
        'name': fields.String(required=True, description='theme name')
    })
    entry = api.model('themes', {
        'name': fields.String(required=True, description='theme name')
    })
    update = api.model('themes', {
        'id': fields.Integer(required=True, description='id'),
        'name': fields.String(required=True, description='theme name')
    })

class AuthDto:
    api = Namespace('auth', description='authentication related operations')
    user_auth = api.model('auth_details', {
        'username': fields.String(required=True, description='The username'),
        'password': fields.String(required=True, description='The user password ')
    })

class UserDto:
    api = Namespace('user', description='user related operations')
    schema = api.model('user', {
        'fullname': fields.String(required=True, description='user fullname'),
        'email': fields.String(required=True, description='user email address'),
        'username': fields.String(required=True, description='user username'),
        'public_id': fields.String(description='user Identifier')
    })
    user = api.model('user', {
        'fullname': fields.String(required=True, description='user fullname'),
        'email': fields.String(required=True, description='user email address'),
        'username': fields.String(required=True, description='user username'),
        'password': fields.String(required=True, description='user password')
    })

class ProxyDto:
    api = Namespace('proxy', description='proxy related operations')
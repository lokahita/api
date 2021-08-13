from flask_restplus import Api
from flask import Blueprint

#from .main.controller.register_controller import api as register_ns
from .main.controller.auth_controller import api as auth_ns
from .main.controller.harvest_controller import api as harvest_ns
from .main.controller.query_controller import api as query_ns
from .main.controller.download_controller import api as download_ns
from .main.controller.countries_controller import api as countries_ns
from .main.controller.continents_controller import api as continents_ns
from .main.controller.regions_controller import api as regions_ns
from .main.controller.organizations_controller import api as organizations_ns
from .main.controller.themes_controller import api as themes_ns
from .main.controller.logs_controller import api as logs_ns
from .main.controller.user_controller import api as user_ns
from .main.controller.statistic_controller import api as statistic_ns
from .main.controller.metadata_controller import api as metadata_ns
from .main.controller.data_controller import api as data_ns
from .main.controller.contribution_controller import api as contribution_ns
from .main.controller.keywords_controller import api as keywords_ns
from .main.controller.proxy_controller import api as proxy_ns

# Import apidoc for monkey patching
from flask_restplus.apidoc import apidoc

URL_PREFIX = '/geoportal/api'

# Make a global change setting the URL prefix for the swaggerui at the module level
apidoc.url_prefix = URL_PREFIX

blueprint = Blueprint('api', __name__,url_prefix=URL_PREFIX)

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization'
    }
}

api = Api(blueprint,
          title='Geoportal API',
          version='1.0',
          description='Geoportal Web Service API End Points',
          authorizations=authorizations,
          security='apikey'
        )

api.add_namespace(auth_ns)
api.add_namespace(continents_ns)
api.add_namespace(contribution_ns)
api.add_namespace(countries_ns)
api.add_namespace(data_ns)
api.add_namespace(download_ns)
api.add_namespace(harvest_ns)
api.add_namespace(keywords_ns)
api.add_namespace(logs_ns)
api.add_namespace(metadata_ns)
api.add_namespace(organizations_ns)
api.add_namespace(proxy_ns)
api.add_namespace(query_ns)
api.add_namespace(regions_ns)
api.add_namespace(themes_ns)
api.add_namespace(statistic_ns)
api.add_namespace(user_ns, path='/user')
#api.add_namespace(user_ns, path='/user')
import uuid
import datetime

from app.main import db
from app.main.model.metadata import Metadata
from app.main.model.user import User

from sqlalchemy import func
from flask.json import jsonify
from owslib.csw import CatalogueServiceWeb
from lxml import etree
import logging

import os
import configparser

from sqlalchemy import create_engine, func, __version__, select
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import create_session

LOGGER = logging.getLogger(__name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'metadata/uploads')
VALIDATION_PATH = os.path.join(os.getcwd(),'iso19139/gmd/gmd.xsd')
PARSER = etree.XMLParser(resolve_entities=False)
from owslib.iso import MD_ImageDescription
import json
from geolinks import sniff_link

class StaticContext(object):
    """core configuration"""
    def __init__(self, prefix='csw30'):
        """initializer"""

        #LOGGER.debug('Initializing static context')
        #self.version = __version__

        self.ogc_schemas_base = 'http://schemas.opengis.net'

        self.parser = PARSER

        self.languages = {
            'en': 'english',
            'fr': 'french',
            'el': 'greek',
        }

        self.response_codes = {
            'OK': '200 OK',
            'NotFound': '404 Not Found',
            'InvalidValue': '400 Invalid property value',
            'OperationParsingFailed': '400 Bad Request',
            'OperationProcessingFailed': '403 Server Processing Failed',
            'OperationNotSupported': '400 Not Implemented',
            'MissingParameterValue': '400 Bad Request',
            'InvalidParameterValue': '400 Bad Request',
            'VersionNegotiationFailed': '400 Bad Request',
            'InvalidUpdateSequence': '400 Bad Request',
            'OptionNotSupported': '400 Not Implemented',
            'NoApplicableCode': '400 Internal Server Error'
        }

        self.namespaces = {
            'atom': 'http://www.w3.org/2005/Atom',
            'csw': 'http://www.opengis.net/cat/csw/2.0.2',
            'csw30': 'http://www.opengis.net/cat/csw/3.0',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dct': 'http://purl.org/dc/terms/',
            'dif': 'http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/',
            'fes20': 'http://www.opengis.net/fes/2.0',
            'fgdc': 'http://www.opengis.net/cat/csw/csdgm',
            'gm03': 'http://www.interlis.ch/INTERLIS2.3',
            'gmd': 'http://www.isotc211.org/2005/gmd',
            'gml': 'http://www.opengis.net/gml',
            'ogc': 'http://www.opengis.net/ogc',
            'os': 'http://a9.com/-/spec/opensearch/1.1/',
            'ows': 'http://www.opengis.net/ows',
            'ows11': 'http://www.opengis.net/ows/1.1',
            'ows20': 'http://www.opengis.net/ows/2.0',
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'soapenv': 'http://www.w3.org/2003/05/soap-envelope',
            'xlink': 'http://www.w3.org/1999/xlink',
            'xs': 'http://www.w3.org/2001/XMLSchema',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
        }

        self.keep_ns_prefixes = [
            'csw', 'dc', 'dct', 'gmd', 'gml', 'ows', 'xs'
        ]

        self.md_core_model = {
            'typename': 'pycsw:CoreMetadata',
            'outputschema': 'http://pycsw.org/metadata',
            'mappings': {
                'pycsw:Identifier': 'identifier',
                # CSW typename (e.g. csw:Record, md:MD_Metadata)
                'pycsw:Typename': 'typename',
                # schema namespace, i.e. http://www.isotc211.org/2005/gmd
                'pycsw:Schema': 'schema',
                # origin of resource, either 'local', or URL to web service
                'pycsw:MdSource': 'mdsource',
                # date of insertion
                'pycsw:InsertDate': 'insert_date',  # date of insertion
                # raw XML metadata
                'pycsw:XML': 'xml',
                # raw metadata payload, xml to be migrated to this in the future
                'pycsw:Metadata': 'metadata',
                # raw metadata payload type, xml as default for now
                'pycsw:MetadataType': 'metadata_type',
                # bag of metadata element and attributes ONLY, no XML tages
                'pycsw:AnyText': 'anytext',
                'pycsw:Language': 'language',
                'pycsw:Title': 'title',
                'pycsw:Abstract': 'abstract',
                'pycsw:Keywords': 'keywords',
                'pycsw:KeywordType': 'keywordstype',
                'pycsw:Format': 'format',
                'pycsw:Source': 'source',
                'pycsw:Date': 'date',
                'pycsw:Modified': 'date_modified',
                'pycsw:Type': 'type',
                # geometry, specified in OGC WKT
                'pycsw:BoundingBox': 'wkt_geometry',
                'pycsw:CRS': 'crs',
                'pycsw:AlternateTitle': 'title_alternate',
                'pycsw:RevisionDate': 'date_revision',
                'pycsw:CreationDate': 'date_creation',
                'pycsw:PublicationDate': 'date_publication',
                'pycsw:OrganizationName': 'organization',
                'pycsw:SecurityConstraints': 'securityconstraints',
                'pycsw:ParentIdentifier': 'parentidentifier',
                'pycsw:TopicCategory': 'topicategory',
                'pycsw:ResourceLanguage': 'resourcelanguage',
                'pycsw:GeographicDescriptionCode': 'geodescode',
                'pycsw:Denominator': 'denominator',
                'pycsw:DistanceValue': 'distancevalue',
                'pycsw:DistanceUOM': 'distanceuom',
                'pycsw:TempExtent_begin': 'time_begin',
                'pycsw:TempExtent_end': 'time_end',
                'pycsw:ServiceType': 'servicetype',
                'pycsw:ServiceTypeVersion': 'servicetypeversion',
                'pycsw:Operation': 'operation',
                'pycsw:CouplingType': 'couplingtype',
                'pycsw:OperatesOn': 'operateson',
                'pycsw:OperatesOnIdentifier': 'operatesonidentifier',
                'pycsw:OperatesOnName': 'operatesoname',
                'pycsw:Degree': 'degree',
                'pycsw:AccessConstraints': 'accessconstraints',
                'pycsw:OtherConstraints': 'otherconstraints',
                'pycsw:Classification': 'classification',
                'pycsw:ConditionApplyingToAccessAndUse': 'conditionapplyingtoaccessanduse',
                'pycsw:Lineage': 'lineage',
                'pycsw:ResponsiblePartyRole': 'responsiblepartyrole',
                'pycsw:SpecificationTitle': 'specificationtitle',
                'pycsw:SpecificationDate': 'specificationdate',
                'pycsw:SpecificationDateType': 'specificationdatetype',
                'pycsw:Creator': 'creator',
                'pycsw:Publisher': 'publisher',
                'pycsw:Contributor': 'contributor',
                'pycsw:Relation': 'relation',
                'pycsw:Platform': 'platform',
                'pycsw:Instrument': 'instrument',
                'pycsw:SensorType': 'sensortype',
                'pycsw:CloudCover': 'cloudcover',
                'pycsw:Bands': 'bands',
                # links: list of dicts with properties: name, description, protocol, url
                'pycsw:Links': 'links',
            }
        }

        self.model = None

        self.models = {
            'csw': {
                'operations_order': [
                    'GetCapabilities', 'DescribeRecord', 'GetDomain',
                    'GetRecords', 'GetRecordById', 'GetRepositoryItem'
                ],
                'operations': {
                    'GetCapabilities': {
                        'methods': {
                            'get': True,
                            'post': True,
                        },
                        'parameters': {
                            'sections': {
                                'values': ['ServiceIdentification', 'ServiceProvider',
                                'OperationsMetadata', 'Filter_Capabilities']
                            }
                        }
                    },
                    'DescribeRecord': {
                        'methods': {
                            'get': True,
                            'post': True,
                        },
                        'parameters': {
                            'schemaLanguage': {
                                'values': ['http://www.w3.org/XML/Schema',
                                           'http://www.w3.org/TR/xmlschema-1/',
                                           'http://www.w3.org/2001/XMLSchema']
                            },
                            'typeName': {
                                'values': ['csw:Record']
                            },
                            'outputFormat': {
                                'values': ['application/xml', 'application/json']
                            }
                        }
                    },
                    'GetRecords': {
                        'methods': {
                            'get': True,
                            'post': True,
                        },
                        'parameters': {
                            'resultType': {
                                'values': ['hits', 'results', 'validate']
                            },
                            'typeNames': {
                                'values': ['csw:Record']
                            },
                            'outputSchema': {
                                'values': ['http://www.opengis.net/cat/csw/2.0.2']
                            },
                            'outputFormat': {
                                'values': ['application/xml', 'application/json']
                            },
                            'CONSTRAINTLANGUAGE': {
                                'values': ['FILTER', 'CQL_TEXT']
                            },
                            'ElementSetName': {
                                'values': ['brief', 'summary', 'full']
                            }
                        },
                        'constraints': {
                        }
                    },
                    'GetRecordById': {
                        'methods': {
                            'get': True,
                            'post': True,
                        },
                        'parameters': {
                            'outputSchema': {
                                'values': ['http://www.opengis.net/cat/csw/2.0.2']
                            },
                            'outputFormat': {
                                'values': ['application/xml', 'application/json']
                            },
                            'ElementSetName': {
                                'values': ['brief', 'summary', 'full']
                            }
                        }
                    },
                    'GetRepositoryItem': {
                        'methods': {
                            'get': True,
                            'post': False,
                        },
                        'parameters': {
                        }
                    }
                },
                'parameters': {
                    'version': {
                        'values': ['2.0.2', '3.0.0']
                    },
                    'service': {
                        'values': ['CSW']
                    }
                },
                'constraints': {
                    'MaxRecordDefault': {
                        'values': ['10']
                    },
                    'PostEncoding': {
                        'values': ['XML', 'SOAP']
                    },
                    'XPathQueryables': {
                        'values': ['allowed']
                    }
                },
                'typenames': {
                    'csw:Record': {
                        'outputschema': 'http://www.opengis.net/cat/csw/2.0.2',
                        'queryables': {
                            'SupportedDublinCoreQueryables': {
                                # map Dublin Core queryables to core metadata model
                                'dc:title':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Title']},
                                'dct:alternative':
                                {'dbcol': self.md_core_model['mappings']['pycsw:AlternateTitle']},
                                'dc:creator':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Creator']},
                                'dc:subject':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Keywords']},
                                'dct:abstract':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Abstract']},
                                'dc:publisher':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Publisher']},
                                'dc:contributor':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Contributor']},
                                'dct:modified':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Modified']},
                                'dc:date':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Date']},
                                'dc:type':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Type']},
                                'dc:format':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Format']},
                                'dc:identifier':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Identifier']},
                                'dc:source':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Source']},
                                'dc:language':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Language']},
                                'dc:relation':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Relation']},
                                'dc:rights':
                                {'dbcol':
                                 self.md_core_model['mappings']['pycsw:AccessConstraints']},
                                'dct:spatial':
                                {'dbcol': self.md_core_model['mappings']['pycsw:CRS']},
                                # bbox and full text map to internal fixed columns
                                'ows:BoundingBox':
                                {'dbcol': self.md_core_model['mappings']['pycsw:BoundingBox']},
                                'csw:AnyText':
                                {'dbcol': self.md_core_model['mappings']['pycsw:AnyText']},
                            }
                        }
                    }
                }
            },
            'csw30': {
                'operations_order': [
                    'GetCapabilities', 'GetDomain', 'GetRecords',
                    'GetRecordById', 'GetRepositoryItem'
                ],
                'operations': {
                    'GetCapabilities': {
                        'methods': {
                            'get': True,
                            'post': True,
                        },
                        'parameters': {
                            'acceptVersions': {
                                'values': ['2.0.2', '3.0.0']
                            },
                            'acceptFormats': {
                                'values': ['text/xml', 'application/xml']
                            },
                            'sections': {
                                'values': ['ServiceIdentification', 'ServiceProvider',
                                'OperationsMetadata', 'Filter_Capabilities', 'All']
                            }
                        }
                    },
                    'GetRecords': {
                        'methods': {
                            'get': True,
                            'post': True,
                        },
                        'parameters': {
                            'typeNames': {
                                'values': ['csw:Record', 'csw30:Record']
                            },
                            'outputSchema': {
                                'values': ['http://www.opengis.net/cat/csw/3.0']
                            },
                            'outputFormat': {
                                'values': ['application/xml', 'application/json', 'application/atom+xml']
                            },
                            'CONSTRAINTLANGUAGE': {
                                'values': ['FILTER', 'CQL_TEXT']
                            },
                            'ElementSetName': {
                                'values': ['brief', 'summary', 'full']
                            }
                        },
                        'constraints': {
                        }
                    },
                    'GetRecordById': {
                        'methods': {
                            'get': True,
                            'post': True,
                        },
                        'parameters': {
                            'outputSchema': {
                                'values': ['http://www.opengis.net/cat/csw/3.0']
                            },
                            'outputFormat': {
                                'values': ['application/xml', 'application/json', 'application/atom+xml']
                            },
                            'ElementSetName': {
                                'values': ['brief', 'summary', 'full']
                            }
                        }
                    },
                    'GetRepositoryItem': {
                        'methods': {
                            'get': True,
                            'post': False,
                        },
                        'parameters': {
                        }
                    }
                },
                'parameters': {
                    'version': {
                        'values': ['2.0.2', '3.0.0']
                    },
                    'service': {
                        'values': ['CSW']
                    }
                },
                'constraints': {
                    'MaxRecordDefault': {
                        'values': ['10']
                    },
                    'PostEncoding': {
                        'values': ['XML', 'SOAP']
                    },
                    'XPathQueryables': {
                        'values': ['allowed']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/OpenSearch': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetCapabilities-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetRecordById-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetRecords-Basic-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetRecords-Distributed-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetRecords-Distributed-KVP': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetRecords-Async-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetRecords-Async-KVP': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetDomain-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/GetDomain-KVP': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Transaction': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Harvest-Basic-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Harvest-Basic-KVP': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Harvest-Async-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Harvest-Async-KVP': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Harvest-Periodic-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Harvest-Periodic-KVP': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Filter-CQL': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Filter-FES-XML': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/Filter-FES-KVP-Advanced': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/SupportedGMLVersions': {
                        'values': ['http://www.opengis.net/gml']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/DefaultSortingAlgorithm': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/CoreQueryables': {
                        'values': ['TRUE']
                    },
                    'http://www.opengis.net/spec/csw/3.0/conf/CoreSortables': {
                        'values': ['TRUE']
                    }
                },
                'typenames': {
                    'csw:Record': {
                        'outputschema': 'http://www.opengis.net/cat/csw/3.0',
                        'queryables': {
                            'SupportedDublinCoreQueryables': {
                                # map Dublin Core queryables to core metadata model
                                'dc:title':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Title']},
                                'dct:alternative':
                                {'dbcol': self.md_core_model['mappings']['pycsw:AlternateTitle']},
                                'dc:creator':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Creator']},
                                'dc:subject':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Keywords']},
                                'dct:abstract':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Abstract']},
                                'dc:publisher':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Publisher']},
                                'dc:contributor':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Contributor']},
                                'dct:modified':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Modified']},
                                'dc:date':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Date']},
                                'dc:type':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Type']},
                                'dc:format':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Format']},
                                'dc:identifier':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Identifier']},
                                'dc:source':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Source']},
                                'dc:language':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Language']},
                                'dc:relation':
                                {'dbcol': self.md_core_model['mappings']['pycsw:Relation']},
                                'dc:rights':
                                {'dbcol':
                                 self.md_core_model['mappings']['pycsw:AccessConstraints']},
                                'dct:spatial':
                                {'dbcol': self.md_core_model['mappings']['pycsw:CRS']},
                                # bbox and full text map to internal fixed columns
                                'ows:BoundingBox':
                                {'dbcol': self.md_core_model['mappings']['pycsw:BoundingBox']},
                                'csw:AnyText':
                                {'dbcol': self.md_core_model['mappings']['pycsw:AnyText']},
                            }
                        }
                    }
                }
            }
        }
        self.set_model(prefix)

    def set_model(self, prefix):
        """sets model given request context"""

        self.model = self.models[prefix]

class EnvInterpolation(configparser.BasicInterpolation):
    """
    Interpolation which expands environment variables in values.
    from: https://stackoverflow.com/a/49529659
    """

    def before_get(self, parser, section, option, value, defaults):
        value = super().before_get(parser, section, option, value, defaults)
        return os.path.expandvars(value)



def validate_xml(xml):
    """Validate XML document against XML Schema"""
    schema = etree.XMLSchema(file=VALIDATION_PATH)

    try:
        valid = etree.parse(xml, PARSER)
        return 'Valid'
    except Exception as err:
        raise RuntimeError('ERROR: %s' % str(err))

class Repository(object):
    _engines = {}

    @classmethod
    def create_engine(clazz, url):
        '''
        SQL Alchemy engines are thread-safe and simple wrappers for connection pools
        https://groups.google.com/forum/#!topic/sqlalchemy/t8i3RSKZGb0
        To reduce startup time we can cache the engine as a class variable in the
        repository object and do database initialization once
        Engines are memoized by url
        '''
        if url not in clazz._engines:
            LOGGER.info('creating new engine: %s', url)
            engine = create_engine('%s' % url, echo=False)

            # load SQLite query bindings
            # This can be directly bound via events
            # for sqlite < 0.7, we need to to this on a per-connection basis
            if engine.name in ['sqlite', 'sqlite3'] and __version__ >= '0.7':
                from sqlalchemy import event
                @event.listens_for(engine, "connect")
                def connect(dbapi_connection, connection_rec):
                    create_custom_sql_functions(dbapi_connection)

            clazz._engines[url] = engine

        return clazz._engines[url]

    ''' Class to interact with underlying repository '''
    def __init__(self, database, context, app_root=None, table='records2', repo_filter=None):
        ''' Initialize repository '''

        self.context = context
        self.filter = repo_filter
        self.fts = False

        # Don't use relative paths, this is hack to get around
        # most wsgi restriction...
        if (app_root and database.startswith('sqlite:///') and
            not database.startswith('sqlite:////')):
            database = database.replace('sqlite:///',
                       'sqlite:///%s%s' % (app_root, os.sep))

        self.engine = Repository.create_engine('%s' % database)

        base = declarative_base(bind=self.engine)

        LOGGER.info('binding ORM to existing database')

        self.postgis_geometry_column = None

        schema_name, table_name = table.rpartition(".")[::2]

        self.dataset = type(
            'dataset',
            (base,),
            {
                "__tablename__": table_name,
                "__table_args__": {
                    "autoload": True,
                    "schema": schema_name or None,
                },
            }
        )

        self.dbtype = self.engine.name

        self.session = create_session(self.engine)

        temp_dbtype = None

        if self.dbtype == 'postgresql':
            # check if PostgreSQL is enabled with PostGIS 1.x
            try:
                self.session.execute(select([func.postgis_version()]))
                temp_dbtype = 'postgresql+postgis+wkt'
                LOGGER.debug('PostgreSQL+PostGIS1+WKT detected')
            except Exception as err:
                LOGGER.exception('PostgreSQL+PostGIS1+WKT detection failed')

            # check if PostgreSQL is enabled with PostGIS 2.x
            try:
                self.session.execute('select(postgis_version())')
                temp_dbtype = 'postgresql+postgis+wkt'
                LOGGER.debug('PostgreSQL+PostGIS2+WKT detected')
            except Exception as err:
                LOGGER.exception('PostgreSQL+PostGIS2+WKT detection failed')

            # check if a native PostGIS geometry column exists
            try:
                result = self.session.execute(
                    "select f_geometry_column "
                    "from geometry_columns "
                    "where f_table_name = '%s' "
                    "and f_geometry_column != 'wkt_geometry' "
                    "limit 1;" % table_name
                )
                row = result.fetchone()
                self.postgis_geometry_column = str(row['f_geometry_column'])
                temp_dbtype = 'postgresql+postgis+native'
                LOGGER.debug('PostgreSQL+PostGIS+Native detected')
            except Exception as err:
                LOGGER.exception('PostgreSQL+PostGIS+Native not picked up: %s')

            # check if a native PostgreSQL FTS GIN index exists
            result = self.session.execute("select relname from pg_class where relname='fts_gin_idx'").scalar()
            self.fts = bool(result)
            LOGGER.debug('PostgreSQL FTS enabled: %r', self.fts)

        if temp_dbtype is not None:
            LOGGER.debug('%s support detected', temp_dbtype)
            self.dbtype = temp_dbtype

        if self.dbtype in ['sqlite', 'sqlite3']:  # load SQLite query bindings
            # <= 0.6 behaviour
            if not __version__ >= '0.7':
                self.connection = self.engine.raw_connection()
                create_custom_sql_functions(self.connection)

        LOGGER.info('setting repository queryables')
        # generate core queryables db and obj bindings
        self.queryables = {}

        for tname in self.context.model['typenames']:
            for qname in self.context.model['typenames'][tname]['queryables']:
                self.queryables[qname] = {}

                for qkey, qvalue in \
                self.context.model['typenames'][tname]['queryables'][qname].items():
                    self.queryables[qname][qkey] = qvalue

        # flatten all queryables
        # TODO smarter way of doing this
        self.queryables['_all'] = {}
        for qbl in self.queryables:
            self.queryables['_all'].update(self.queryables[qbl])

        self.queryables['_all'].update(self.context.md_core_model['mappings'])

    def _create_values(self, values):
        value_dict = {}
        for num, value in enumerate(values):
            print(num, ' ', value)
            value_dict['pvalue%d' % num] = value
        return value_dict

    def query_ids(self, ids):
        ''' Query by list of identifiers '''

        column = getattr(self.dataset, \
        self.context.md_core_model['mappings']['pycsw:Identifier'])

        query = self.session.query(self.dataset).filter(column.in_(ids))
        return self._get_repo_filter(query).all()

    def query_domain(self, domain, typenames, domainquerytype='list',
        count=False):
        ''' Query by property domain values '''

        domain_value = getattr(self.dataset, domain)

        if domainquerytype == 'range':
            LOGGER.info('Generating property name range values')
            query = self.session.query(func.min(domain_value),
                                       func.max(domain_value))
        else:
            if count:
                LOGGER.info('Generating property name frequency counts')
                query = self.session.query(getattr(self.dataset, domain),
                    func.count(domain_value)).group_by(domain_value)
            else:
                query = self.session.query(domain_value).distinct()
        return self._get_repo_filter(query).all()

    def query_insert(self, direction='max'):
        ''' Query to get latest (default) or earliest update to repository '''
        column = getattr(self.dataset, \
        self.context.md_core_model['mappings']['pycsw:InsertDate'])

        if direction == 'min':
            return self._get_repo_filter(self.session.query(func.min(column))).first()[0]
        # else default max
        return self._get_repo_filter(self.session.query(func.max(column))).first()[0]

    def query_source(self, source):
        ''' Query by source '''
        column = getattr(self.dataset, \
        self.context.md_core_model['mappings']['pycsw:Source'])

        query = self.session.query(self.dataset).filter(column == source)
        return self._get_repo_filter(query).all()

    def query(self, constraint, sortby=None, typenames=None,
        maxrecords=10, startposition=0):
        ''' Query records from underlying repository '''

        # run the raw query and get total
        if 'where' in constraint:  # GetRecords with constraint
            LOGGER.debug('constraint detected')
            query = self.session.query(self.dataset).filter(
            text(constraint['where'])).params(self._create_values(constraint['values']))
        else:  # GetRecords sans constraint
            LOGGER.debug('No constraint detected')
            query = self.session.query(self.dataset)

        total = self._get_repo_filter(query).count()

        if util.ranking_pass:  #apply spatial ranking
            #TODO: Check here for dbtype so to extract wkt from postgis native to wkt
            LOGGER.debug('spatial ranking detected')
            LOGGER.debug('Target WKT: %s', getattr(self.dataset, self.context.md_core_model['mappings']['pycsw:BoundingBox']))
            LOGGER.debug('Query WKT: %s', util.ranking_query_geometry)
            query = query.order_by(func.get_spatial_overlay_rank(getattr(self.dataset, self.context.md_core_model['mappings']['pycsw:BoundingBox']), util.ranking_query_geometry).desc())
            #trying to make this wsgi safe
            util.ranking_pass = False
            util.ranking_query_geometry = ''

        if sortby is not None:  # apply sorting
            LOGGER.debug('sorting detected')
            #TODO: Check here for dbtype so to extract wkt from postgis native to wkt
            sortby_column = getattr(self.dataset, sortby['propertyname'])

            if sortby['order'] == 'DESC':  # descending sort
                if 'spatial' in sortby and sortby['spatial']:  # spatial sort
                    query = query.order_by(func.get_geometry_area(sortby_column).desc())
                else:  # aspatial sort
                    query = query.order_by(sortby_column.desc())
            else:  # ascending sort
                if 'spatial' in sortby and sortby['spatial']:  # spatial sort
                    query = query.order_by(func.get_geometry_area(sortby_column))
                else:  # aspatial sort
                    query = query.order_by(sortby_column)

        # always apply limit and offset
        return [str(total), self._get_repo_filter(query).limit(
        maxrecords).offset(startposition).all()]

    def insert(self, record, source, insert_date):
        ''' Insert a record into the repository '''

        if isinstance(record.xml, bytes):
            LOGGER.debug('Decoding bytes to unicode')
            record.xml = record.xml.decode()

        try:
            self.session.begin()
            self.session.add(record)
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            raise

    def update(self, record=None, recprops=None, constraint=None):
        ''' Update a record in the repository based on identifier '''

        if record is not None:
            identifier = getattr(record,
            self.context.md_core_model['mappings']['pycsw:Identifier'])
            xml = getattr(self.dataset,
            self.context.md_core_model['mappings']['pycsw:XML'])
            anytext = getattr(self.dataset,
            self.context.md_core_model['mappings']['pycsw:AnyText'])

        if recprops is None and constraint is None:  # full update
            LOGGER.debug('full update')
            update_dict = dict([(getattr(self.dataset, key),
            getattr(record, key)) \
            for key in record.__dict__.keys() if key != '_sa_instance_state'])

            try:
                self.session.begin()
                self._get_repo_filter(self.session.query(self.dataset)).filter_by(
                identifier=identifier).update(update_dict, synchronize_session='fetch')
                self.session.commit()
            except Exception as err:
                self.session.rollback()
                msg = 'Cannot commit to repository'
                LOGGER.exception(msg)
                raise RuntimeError(msg)
        else:  # update based on record properties
            LOGGER.debug('property based update')
            try:
                rows = rows2 = 0
                self.session.begin()
                for rpu in recprops:
                    # update queryable column and XML document via XPath
                    if 'xpath' not in rpu['rp']:
                        self.session.rollback()
                        raise RuntimeError('XPath not found for property %s' % rpu['rp']['name'])
                    if 'dbcol' not in rpu['rp']:
                        self.session.rollback()
                        raise RuntimeError('property not found for XPath %s' % rpu['rp']['name'])
                    rows += self._get_repo_filter(self.session.query(self.dataset)).filter(
                        text(constraint['where'])).params(self._create_values(constraint['values'])).update({
                            getattr(self.dataset,
                            rpu['rp']['dbcol']): rpu['value'],
                            'xml': func.update_xpath(str(self.context.namespaces),
                                   getattr(self.dataset,
                                   self.context.md_core_model['mappings']['pycsw:XML']),
                                   str(rpu)),
                        }, synchronize_session='fetch')
                    # then update anytext tokens
                    rows2 += self._get_repo_filter(self.session.query(self.dataset)).filter(
                        text(constraint['where'])).params(self._create_values(constraint['values'])).update({
                            'anytext': func.get_anytext(getattr(
                            self.dataset, self.context.md_core_model['mappings']['pycsw:XML']))
                        }, synchronize_session='fetch')
                self.session.commit()
                return rows
            except Exception as err:
                self.session.rollback()
                msg = 'Cannot commit to repository'
                LOGGER.exception(msg)
                raise RuntimeError(msg)

    def delete(self, constraint):
        ''' Delete a record from the repository '''
        print(" xxxxx ", constraint)
        print(self._create_values(constraint['values']))
        print(text(constraint['where']).params(self._create_values(constraint['values'])))
        
        try:
            self.session.begin()
            rows = self._get_repo_filter(self.session.query(self.dataset)).filter(
            text(constraint['where'])).params(self._create_values(constraint['values']))
           
            print(rows)
            parentids = []
            for row in rows:  # get ids
                parentids.append(getattr(row,
                self.context.md_core_model['mappings']['pycsw:Identifier']))

            rows=rows.delete(synchronize_session='fetch')

            if rows > 0:
                LOGGER.debug('Deleting all child records')
                # delete any child records which had this record as a parent
                rows += self._get_repo_filter(self.session.query(self.dataset)).filter(
                    getattr(self.dataset,
                    self.context.md_core_model['mappings']['pycsw:ParentIdentifier']).in_(parentids)).delete(
                    synchronize_session='fetch')

            self.session.commit()
        except Exception as err:
            self.session.rollback()
            msg = 'Cannot commit to repository'
            LOGGER.exception(msg)
            raise RuntimeError(msg)

        return rows

    def _get_repo_filter(self, query):
        ''' Apply repository wide side filter / mask query '''
        if self.filter is not None:
            return query.filter(text(self.filter))
        return query


def create_custom_sql_functions(connection):
    """Register custom functions on the database connection."""

    inspect_function = inspect.getfullargspec

    for function_object in [
        query_spatial,
        update_xpath,
        util.get_anytext,
        get_geometry_area,
        get_spatial_overlay_rank
    ]:
        argspec = inspect_function(function_object)
        connection.create_function(
            function_object.__name__,
            len(argspec.args),
            function_object
        )


def query_spatial(bbox_data_wkt, bbox_input_wkt, predicate, distance):
    """Perform spatial query
    Parameters
    ----------
    bbox_data_wkt: str
        Well-Known Text representation of the data being queried
    bbox_input_wkt: str
        Well-Known Text representation of the input being queried
    predicate: str
        Spatial predicate to use in query
    distance: int or float or str
        Distance parameter for when using either of ``beyond`` or ``dwithin``
        predicates.
    Returns
    -------
    str
        Either ``true`` or ``false`` depending on the result of the spatial
        query
    Raises
    ------
    RuntimeError
        If an invalid predicate is used
    """

    try:
        bbox1 = loads(bbox_data_wkt.split(';')[-1])
        bbox2 = loads(bbox_input_wkt)
        if predicate == 'bbox':
            result = bbox1.intersects(bbox2)
        elif predicate == 'beyond':
            result = bbox1.distance(bbox2) > float(distance)
        elif predicate == 'contains':
            result = bbox1.contains(bbox2)
        elif predicate == 'crosses':
            result = bbox1.crosses(bbox2)
        elif predicate == 'disjoint':
            result = bbox1.disjoint(bbox2)
        elif predicate == 'dwithin':
            result = bbox1.distance(bbox2) <= float(distance)
        elif predicate == 'equals':
            result = bbox1.equals(bbox2)
        elif predicate == 'intersects':
            result = bbox1.intersects(bbox2)
        elif predicate == 'overlaps':
            result = bbox1.intersects(bbox2) and not bbox1.touches(bbox2)
        elif predicate == 'touches':
            result = bbox1.touches(bbox2)
        elif predicate == 'within':
            result = bbox1.within(bbox2)
        else:
            raise RuntimeError(
                'Invalid spatial query predicate: %s' % predicate)
    except (AttributeError, ValueError, ReadingError, TypeError):
        result = False
    return "true" if result else "false"


def update_xpath(nsmap, xml, recprop):
    """Update XML document XPath values"""

    if isinstance(xml, bytes) or isinstance(xml, str):
        # serialize to lxml
        xml = etree.fromstring(xml, PARSER)

    recprop = eval(recprop)
    nsmap = eval(nsmap)
    try:
        nodes = xml.xpath(recprop['rp']['xpath'], namespaces=nsmap)
        if len(nodes) > 0:  # matches
            for node1 in nodes:
                if node1.text != recprop['value']:  # values differ, update
                    node1.text = recprop['value']
    except Exception as err:
        LOGGER.warning(err)
        raise RuntimeError('ERROR: %s' % str(err))

    return etree.tostring(xml)


def get_geometry_area(geometry):
    """Derive area of a given geometry"""
    try:
        if geometry is not None:
            return str(loads(geometry).area)
        return '0'
    except:
        return '0'


def get_spatial_overlay_rank(target_geometry, query_geometry):
    """Derive spatial overlay rank for geospatial search as per Lanfear (2006)
    http://pubs.usgs.gov/of/2006/1279/2006-1279.pdf"""

    #TODO: Add those parameters to config file
    kt = 1.0
    kq = 1.0
    if target_geometry is not None and query_geometry is not None:
        try:
            q_geom = loads(query_geometry)
            t_geom = loads(target_geometry)
            Q = q_geom.area
            T = t_geom.area
            if any(item == 0.0 for item in [Q, T]):
                LOGGER.warning('Geometry has no area')
                return '0'
            X = t_geom.intersection(q_geom).area
            if kt == 1.0 and kq == 1.0:
                LOGGER.debug('Spatial Rank: %s', str((X/Q)*(X/T)))
                return str((X/Q)*(X/T))
            else:
                LOGGER.debug('Spatial Rank: %s', str(((X/Q)**kq)*((X/T)**kt)))
                return str(((X/Q)**kq)*((X/T)**kt))
        except Exception as err:
            LOGGER.warning('Cannot derive spatial overlay ranking %s', err)
            return '0'
    return '0'
import uuid
import time

def http_request(method, url, request=None, timeout=30):
    """Perform HTTP request"""
    if method == 'POST':
        return http_post(url, request, timeout=timeout)
    else:  # GET
        request = Request(url)
        request.add_header('User-Agent', 'pycsw (https://pycsw.org/)')
        return urlopen(request, timeout=timeout).read()

def get_today_and_now():
    """Get the date, right now, in ISO8601"""
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())

def parse_record(context, record, repos=None,
    mtype='http://www.opengis.net/cat/csw/2.0.2',
    identifier=None, pagesize=10):
    ''' parse metadata '''

    if identifier is None:
        identifier = uuid.uuid4().urn

    # parse web services
    if (mtype == 'http://www.opengis.net/cat/csw/2.0.2' and
        isinstance(record, str) and record.startswith('http')):
        LOGGER.info('CSW service detected, fetching via HTTP')
        # CSW service, not csw:Record
        try:
            return _parse_csw(context, repos, record, identifier, pagesize)
        except Exception as err:
            # TODO: implement better exception handling
            if str(err).find('ExceptionReport') != -1:
                msg = 'CSW harvesting error: %s' % str(err)
                LOGGER.exception(msg)
                raise RuntimeError(msg)
            LOGGER.debug('Not a CSW, attempting to fetch Dublin Core')
            try:
                content = http_request('GET', record)
            except Exception as err:
                raise RuntimeError('HTTP error: %s' % str(err))
            return [_parse_dc(context, repos, etree.fromstring(content, context.parser))]

    elif mtype == 'urn:geoss:waf':  # WAF
        LOGGER.info('WAF detected, fetching via HTTP')
        return _parse_waf(context, repos, record, identifier)

    elif mtype == 'http://www.opengis.net/wms':  # WMS
        LOGGER.info('WMS detected, fetching via OWSLib')
        return _parse_wms(context, repos, record, identifier)

    elif mtype == 'http://www.opengis.net/wmts/1.0':  # WMTS
        LOGGER.info('WMTS 1.0.0 detected, fetching via OWSLib')
        return _parse_wmts(context, repos, record, identifier)

    elif mtype == 'http://www.opengis.net/wps/1.0.0':  # WPS
        LOGGER.info('WPS detected, fetching via OWSLib')
        return _parse_wps(context, repos, record, identifier)

    elif mtype == 'http://www.opengis.net/wfs':  # WFS 1.1.0
        LOGGER.info('WFS detected, fetching via OWSLib')
        return _parse_wfs(context, repos, record, identifier, '1.1.0')

    elif mtype == 'http://www.opengis.net/wfs/2.0':  # WFS 2.0.0
        LOGGER.info('WFS detected, fetching via OWSLib')
        return _parse_wfs(context, repos, record, identifier, '2.0.0')

    elif mtype == 'http://www.opengis.net/wcs':  # WCS
        LOGGER.info('WCS detected, fetching via OWSLib')
        return _parse_wcs(context, repos, record, identifier)

    elif mtype == 'http://www.opengis.net/sos/1.0':  # SOS 1.0.0
        LOGGER.info('SOS 1.0.0 detected, fetching via OWSLib')
        return _parse_sos(context, repos, record, identifier, '1.0.0')

    elif mtype == 'http://www.opengis.net/sos/2.0':  # SOS 2.0.0
        LOGGER.info('SOS 2.0.0 detected, fetching via OWSLib')
        return _parse_sos(context, repos, record, identifier, '2.0.0')

    elif (mtype == 'http://www.opengis.net/cat/csw/csdgm' and
          record.startswith('http')):  # FGDC
        LOGGER.info('FGDC detected, fetching via HTTP')
        record = http_request('GET', record)

    return _parse_metadata(context, repos, record)

def _get(context, obj, name):
    ''' convenience method to get values '''
    return getattr(obj, context.md_core_model['mappings'][name])

def _set(context, obj, name, value):
    ''' convenience method to set values '''
    setattr(obj, context.md_core_model['mappings'][name], value)

def _parse_metadata(context, repos, record):
    """parse metadata formats"""

    if isinstance(record, bytes) or isinstance(record, str):
        exml = etree.fromstring(record, context.parser)
    else:  # already serialized to lxml
        if hasattr(record, 'getroot'):  # standalone document
            exml = record.getroot()
        else:  # part of a larger document
            exml = record

    root = exml.tag

    LOGGER.info('Serialized metadata, parsing content model')

    if root == '{%s}MD_Metadata' % context.namespaces['gmd']:  # ISO
        return [_parse_iso(context, repos, exml)]
    elif root == '{http://www.isotc211.org/2005/gmi}MI_Metadata':
        # ISO Metadata for Imagery
        return [_parse_iso(context, repos, exml)]
    elif root == 'metadata':  # FGDC
        return [_parse_fgdc(context, repos, exml)]
    elif root == '{%s}TRANSFER' % context.namespaces['gm03']:  # GM03
        return [_parse_gm03(context, repos, exml)]
    elif root == '{http://www.geocat.ch/2008/che}CHE_MD_Metadata': # GM03 ISO profile
        return [_parse_iso(context, repos, exml)]
    elif root == '{%s}Record' % context.namespaces['csw']:  # Dublin Core CSW
        return [_parse_dc(context, repos, exml)]
    elif root == '{%s}RDF' % context.namespaces['rdf']:  # Dublin Core RDF
        return [_parse_dc(context, repos, exml)]
    elif root == '{%s}DIF' % context.namespaces['dif']:  # DIF
        pass  # TODO
    else:
        raise RuntimeError('Unsupported metadata format')


def _parse_csw(context, repos, record, identifier, pagesize=10):

    from owslib.csw import CatalogueServiceWeb

    recobjs = []  # records
    serviceobj = repos.dataset()

    # if init raises error, this might not be a CSW
    md = CatalogueServiceWeb(record, timeout=60)

    LOGGER.info('Setting CSW service metadata')
    # generate record of service instance
    _set(context, serviceobj, 'pycsw:Identifier', identifier)
    _set(context, serviceobj, 'pycsw:Typename', 'csw:Record')
    _set(context, serviceobj, 'pycsw:Schema', 'http://www.opengis.net/cat/csw/2.0.2')
    _set(context, serviceobj, 'pycsw:MdSource', record)
    _set(context, serviceobj, 'pycsw:InsertDate', get_today_and_now())
    _set(
        context,
        serviceobj,
        'pycsw:AnyText',
        get_anytext(md._exml)
    )
    _set(context, serviceobj, 'pycsw:Type', 'service')
    _set(context, serviceobj, 'pycsw:Title', md.identification.title)
    _set(context, serviceobj, 'pycsw:Abstract', md.identification.abstract)
    _set(context, serviceobj, 'pycsw:Keywords', ','.join(md.identification.keywords))
    _set(context, serviceobj, 'pycsw:Creator', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:Publisher', md.provider.name)
    _set(context, serviceobj, 'pycsw:Contributor', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:OrganizationName', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:AccessConstraints', md.identification.accessconstraints)
    _set(context, serviceobj, 'pycsw:OtherConstraints', md.identification.fees)
    _set(context, serviceobj, 'pycsw:Source', record)
    _set(context, serviceobj, 'pycsw:Format', md.identification.type)

    _set(context, serviceobj, 'pycsw:ServiceType', 'OGC:CSW')
    _set(context, serviceobj, 'pycsw:ServiceTypeVersion', md.identification.version)
    _set(context, serviceobj, 'pycsw:Operation', ','.join([d.name for d in md.operations]))
    _set(context, serviceobj, 'pycsw:CouplingType', 'tight')

    links = [{
        'name': identifier,
        'description': 'OGC-CSW Catalogue Service for the Web',
        'protocol': 'OGC:CSW',
        'url': md.url
    }]

    _set(context, serviceobj, 'pycsw:Links', json.dumps(links))
    _set(context, serviceobj, 'pycsw:XML', caps2iso(serviceobj, md, context))
    _set(context, serviceobj, 'pycsw:MetadataType', 'application/xml')

    recobjs.append(serviceobj)

    # get all supported typenames of metadata
    # so we can harvest the entire CSW

    # try for ISO, settle for Dublin Core
    csw_typenames = 'csw:Record'
    csw_outputschema = 'http://www.opengis.net/cat/csw/2.0.2'

    grop = md.get_operation_by_name('GetRecords')
    if all(['gmd:MD_Metadata' in grop.parameters['typeNames']['values'],
            'http://www.isotc211.org/2005/gmd' in grop.parameters['outputSchema']['values']]):
        LOGGER.debug('CSW supports ISO')
        csw_typenames = 'gmd:MD_Metadata'
        csw_outputschema = 'http://www.isotc211.org/2005/gmd'


    # now get all records
    # get total number of records to loop against

    try:
        md.getrecords2(typenames=csw_typenames, resulttype='hits',
                       outputschema=csw_outputschema)
        matches = md.results['matches']
    except:  # this is a CSW, but server rejects query
        raise RuntimeError(md.response)

    if pagesize > matches:
        pagesize = matches

    LOGGER.info('Harvesting %d CSW records', matches)

    # loop over all catalogue records incrementally
    for r in range(1, matches+1, pagesize):
        try:
            md.getrecords2(typenames=csw_typenames, startposition=r,
                           maxrecords=pagesize, outputschema=csw_outputschema, esn='full')
        except Exception as err:  # this is a CSW, but server rejects query
            raise RuntimeError(md.response)
        for k, v in md.records.items():
            # try to parse metadata
            try:
                LOGGER.info('Parsing metadata record: %s', v.xml)
                if csw_typenames == 'gmd:MD_Metadata':
                    recobjs.append(_parse_iso(context, repos,
                                              etree.fromstring(v.xml, context.parser)))
                else:
                    recobjs.append(_parse_dc(context, repos,
                                             etree.fromstring(v.xml, context.parser)))
            except Exception as err:  # parsing failed for some reason
                LOGGER.exception('Metadata parsing failed')

    return recobjs

def _parse_waf(context, repos, record, identifier):

    recobjs = []

    content = http_request('GET', record)

    LOGGER.debug(content)

    try:
        parser = etree.HTMLParser()
        tree = etree.fromstring(content, parser)
    except Exception as err:
        raise Exception('Could not parse WAF: %s' % str(err))

    up = urlparse(record)
    links = []

    LOGGER.info('collecting links')
    for link in tree.xpath('//a/@href'):
        link = link.strip()
        if not link:
            continue
        if link.find('?') != -1:
            continue
        if not link.endswith('.xml'):
            LOGGER.debug('Skipping, not .xml')
            continue
        if '/' in link:  # path is embedded in link
            if link[-1] == '/':  # directory, skip
                continue
            if link[0] == '/':
                # strip path of WAF URL
                link = '%s://%s%s' % (up.scheme, up.netloc, link)
        else:  # tack on href to WAF URL
            link = '%s/%s' % (record, link)
        LOGGER.debug('URL is: %s', link)
        links.append(link)

    LOGGER.debug('%d links found', len(links))
    for link in links:
        LOGGER.info('Processing link %s', link)
        # fetch and parse
        linkcontent = http_request('GET', link)
        recobj = _parse_metadata(context, repos, linkcontent)[0]
        recobj.source = link
        recobj.mdsource = link
        recobjs.append(recobj)

    return recobjs

def _parse_wms(context, repos, record, identifier):

    from owslib.wms import WebMapService

    recobjs = []
    serviceobj = repos.dataset()

    md = WebMapService(record)
    try:
        md = WebMapService(record, version='1.3.0')
    except Exception as err:
        LOGGER.info('Looks like WMS 1.3.0 is not supported; trying 1.1.1', err)
        md = WebMapService(record)

    # generate record of service instance
    _set(context, serviceobj, 'pycsw:Identifier', identifier)
    _set(context, serviceobj, 'pycsw:Typename', 'csw:Record')
    _set(context, serviceobj, 'pycsw:Schema', 'http://www.opengis.net/wms')
    _set(context, serviceobj, 'pycsw:MdSource', record)
    _set(context, serviceobj, 'pycsw:InsertDate', get_today_and_now())
    _set(
        context,
        serviceobj,
        'pycsw:AnyText',
        get_anytext(md.getServiceXML())
    )
    _set(context, serviceobj, 'pycsw:Type', 'service')
    _set(context, serviceobj, 'pycsw:Title', md.identification.title)
    _set(context, serviceobj, 'pycsw:Abstract', md.identification.abstract)
    _set(context, serviceobj, 'pycsw:Keywords', ','.join(md.identification.keywords))
    _set(context, serviceobj, 'pycsw:Creator', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:Publisher', md.provider.name)
    _set(context, serviceobj, 'pycsw:Contributor', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:OrganizationName', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:AccessConstraints', md.identification.accessconstraints)
    _set(context, serviceobj, 'pycsw:OtherConstraints', md.identification.fees)
    _set(context, serviceobj, 'pycsw:Source', record)
    _set(context, serviceobj, 'pycsw:Format', md.identification.type)
    for c in md.contents:
        if md.contents[c].parent is None:
            bbox = md.contents[c].boundingBoxWGS84
            tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
            _set(context, serviceobj, 'pycsw:BoundingBox', bbox2wktpolygon(tmp))
            break
    _set(context, serviceobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
    _set(context, serviceobj, 'pycsw:DistanceUOM', 'degrees')
    _set(context, serviceobj, 'pycsw:ServiceType', 'OGC:WMS')
    _set(context, serviceobj, 'pycsw:ServiceTypeVersion', md.identification.version)
    _set(context, serviceobj, 'pycsw:Operation', ','.join([d.name for d in md.operations]))
    _set(context, serviceobj, 'pycsw:OperatesOn', ','.join(list(md.contents)))
    _set(context, serviceobj, 'pycsw:CouplingType', 'tight')

    links = [{
        'name': identifier,
        'description': 'OGC-WMS Web Map Service',
        'protocol': 'OGC:WMS',
        'url': md.url
    }]

    _set(context, serviceobj, 'pycsw:Links', json.dumps(links))
    _set(context, serviceobj, 'pycsw:XML', caps2iso(serviceobj, md, context))
    _set(context, serviceobj, 'pycsw:MetadataType', 'application/xml')

    recobjs.append(serviceobj)

    # generate record foreach layer

    LOGGER.info('Harvesting %d WMS layers', len(md.contents))

    for layer in md.contents:
        recobj = repos.dataset()
        identifier2 = '%s-%s' % (identifier, md.contents[layer].name)
        _set(context, recobj, 'pycsw:Identifier', identifier2)
        _set(context, recobj, 'pycsw:Typename', 'csw:Record')
        _set(context, recobj, 'pycsw:Schema', 'http://www.opengis.net/wms')
        _set(context, recobj, 'pycsw:MdSource', record)
        _set(context, recobj, 'pycsw:InsertDate', get_today_and_now())
        _set(context, recobj, 'pycsw:Type', 'dataset')
        _set(context, recobj, 'pycsw:ParentIdentifier', identifier)
        _set(context, recobj, 'pycsw:Title', md.contents[layer].title)
        _set(context, recobj, 'pycsw:Abstract', md.contents[layer].abstract)
        _set(context, recobj, 'pycsw:Keywords', ','.join(md.contents[layer].keywords))

        _set(
            context,
            recobj,
            'pycsw:AnyText',
            get_anytext([
                md.contents[layer].title,
                md.contents[layer].abstract,
                ','.join(md.contents[layer].keywords)
            ])
        )

        bbox = md.contents[layer].boundingBoxWGS84
        if bbox is not None:
            tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
            _set(context, recobj, 'pycsw:BoundingBox', bbox2wktpolygon(tmp))
            _set(context, recobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
            _set(context, recobj, 'pycsw:DistanceUOM', 'degrees')
        else:
            bbox = md.contents[layer].boundingBox
            if bbox:
                tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
                _set(context, recobj, 'pycsw:BoundingBox',  bbox2wktpolygon(tmp))
                _set(context, recobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:%s' % \
                bbox[-1].split(':')[1])

        times = md.contents[layer].timepositions
        if times is not None:  # get temporal extent
            time_begin = time_end = None
            if len(times) == 1 and len(times[0].split('/')) > 1:
                time_envelope = times[0].split('/')
                time_begin = time_envelope[0]
                time_end = time_envelope[1]
            elif len(times) > 1:  # get first/last
                time_begin = times[0]
                time_end = times[-1]
            if all([time_begin is not None, time_end is not None]):
                _set(context, recobj, 'pycsw:TempExtent_begin', time_begin)
                _set(context, recobj, 'pycsw:TempExtent_end', time_end)

        params = {
            'service': 'WMS',
            'version': '1.1.1',
            'request': 'GetMap',
            'layers': md.contents[layer].name,
            'format': 'image/png',
            'height': '200',
            'width': '200',
            'srs': 'EPSG:4326',
            'bbox':  '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3]),
            'styles': ''
        }

        links = [{
            'name': md.contents[layer].name,
            'description': 'OGC-Web Map Service',
            'protocol': 'OGC:WMS',
            'url': md.url
            }, {
            'name': md.contents[layer].name,
            'description': 'Web image thumbnail (URL)',
            'protocol': 'WWW:LINK-1.0-http--image-thumbnail',
            'url': build_get_url(md.url, params)
        }]

        _set(context, recobj, 'pycsw:Links', json.dumps(links))
        _set(context, recobj, 'pycsw:XML', caps2iso(recobj, md, context))
        _set(context, recobj, 'pycsw:MetadataType', 'application/xml')

        recobjs.append(recobj)

    return recobjs

def _parse_wmts(context, repos, record, identifier):

    from owslib.wmts import WebMapTileService

    recobjs = []
    serviceobj = repos.dataset()

    md = WebMapTileService(record)
    # generate record of service instance
    _set(context, serviceobj, 'pycsw:Identifier', identifier)
    _set(context, serviceobj, 'pycsw:Typename', 'csw:Record')
    _set(context, serviceobj, 'pycsw:Schema', 'http://www.opengis.net/wmts/1.0')
    _set(context, serviceobj, 'pycsw:MdSource', record)
    _set(context, serviceobj, 'pycsw:InsertDate',  get_today_and_now())
    _set(
        context,
        serviceobj,
        'pycsw:AnyText',
        get_anytext(md.getServiceXML())
    )
    _set(context, serviceobj, 'pycsw:Type', 'service')
    _set(context, serviceobj, 'pycsw:Title', md.identification.title)
    _set(context, serviceobj, 'pycsw:Abstract', md.identification.abstract)
    _set(context, serviceobj, 'pycsw:Keywords', ','.join(md.identification.keywords))
    _set(context, serviceobj, 'pycsw:Creator', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:Publisher', md.provider.name)
    _set(context, serviceobj, 'pycsw:Contributor', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:OrganizationName', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:AccessConstraints', md.identification.accessconstraints)
    _set(context, serviceobj, 'pycsw:OtherConstraints', md.identification.fees)
    _set(context, serviceobj, 'pycsw:Source', record)
    _set(context, serviceobj, 'pycsw:Format', md.identification.type)

    for c in md.contents:
        if md.contents[c].parent is None:
            bbox = md.contents[c].boundingBoxWGS84
            tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
            _set(context, serviceobj, 'pycsw:BoundingBox',  bbox2wktpolygon(tmp))
            break
    _set(context, serviceobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
    _set(context, serviceobj, 'pycsw:DistanceUOM', 'degrees')
    _set(context, serviceobj, 'pycsw:ServiceType', 'OGC:WMTS')
    _set(context, serviceobj, 'pycsw:ServiceTypeVersion', md.identification.version)
    _set(context, serviceobj, 'pycsw:Operation', ','.join([d.name for d in md.operations]))
    _set(context, serviceobj, 'pycsw:OperatesOn', ','.join(list(md.contents)))
    _set(context, serviceobj, 'pycsw:CouplingType', 'tight')

    links = [{
        'name': identifier,
        'description': 'OGC-WMTS Web Map Service',
        'protocol': 'OGC:WMTS',
        'url': md.url
    }]

    _set(context, serviceobj, 'pycsw:Links', json.dumps(links))
    _set(context, serviceobj, 'pycsw:XML', caps2iso(serviceobj, md, context))
    _set(context, serviceobj, 'pycsw:MetadataType', 'application/xml')

    recobjs.append(serviceobj)

    # generate record for each layer

    LOGGER.debug('Harvesting %d WMTS layers', len(md.contents))

    for layer in md.contents:
        recobj = repos.dataset()
        keywords = ''
        identifier2 = '%s-%s' % (identifier, md.contents[layer].name)
        _set(context, recobj, 'pycsw:Identifier', identifier2)
        _set(context, recobj, 'pycsw:Typename', 'csw:Record')
        _set(context, recobj, 'pycsw:Schema', 'http://www.opengis.net/wmts/1.0')
        _set(context, recobj, 'pycsw:MdSource', record)
        _set(context, recobj, 'pycsw:InsertDate',  get_today_and_now())
        _set(context, recobj, 'pycsw:Type', 'dataset')
        _set(context, recobj, 'pycsw:ParentIdentifier', identifier)
        if md.contents[layer].title:
             _set(context, recobj, 'pycsw:Title', md.contents[layer].title)
        else:
            _set(context, recobj, 'pycsw:Title', "")
        if md.contents[layer].abstract:
            _set(context, recobj, 'pycsw:Abstract', md.contents[layer].abstract)
        else:
            _set(context, recobj, 'pycsw:Abstract', "")
        if hasattr(md.contents[layer], 'keywords'):  # safeguard against older OWSLib installs
            keywords = ','.join(md.contents[layer].keywords)
        _set(context, recobj, 'pycsw:Keywords', keywords)

        _set(
            context,
            recobj,
            'pycsw:AnyText',
              get_anytext([
                 md.contents[layer].title,
                 md.contents[layer].abstract,
                 ','.join(keywords)
             ])
        )

        bbox = md.contents[layer].boundingBoxWGS84

        if bbox is not None:
            tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
            _set(context, recobj, 'pycsw:BoundingBox', bbox2wktpolygon(tmp))
            _set(context, recobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
            _set(context, recobj, 'pycsw:DistanceUOM', 'degrees')
        else:
            bbox = md.contents[layer].boundingBox
            if bbox:
                tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
                _set(context, recobj, 'pycsw:BoundingBox', bbox2wktpolygon(tmp))
                _set(context, recobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:%s' % \
                bbox[-1].split(':')[1])


        params = {
            'service': 'WMTS',
            'version': '1.0.0',
            'request': 'GetTile',
            'layer': md.contents[layer].name,
        }

        links = [{
           'name': md.contents[layer].name,
           'description': 'OGC-Web Map Tile Service',
           'protocol': 'OGC:WMTS',
           'url': md.url
           }, {
           'name': md.contents[layer].name,
           'description': 'Web image thumbnail (URL)',
           'protocol': 'WWW:LINK-1.0-http--image-thumbnai',
           'url': build_get_url(md.url, params)
        }]

        _set(context, recobj, 'pycsw:Links', json.dumps(links))
        _set(context, recobj, 'pycsw:XML', caps2iso(recobj, md, context))
        _set(context, recobj, 'pycsw:MetadataType', 'application/xml')

        recobjs.append(recobj)

    return recobjs


def _parse_wfs(context, repos, record, identifier, version):

    import requests
    from owslib.wfs import WebFeatureService

    bboxs = []
    recobjs = []
    serviceobj = repos.dataset()

    try:
        md = WebFeatureService(record, version)
    except requests.exceptions.HTTPError as err:
        raise
    except Exception as err:
        if version == '1.1.0':
            md = WebFeatureService(record, '1.0.0')

    # generate record of service instance
    _set(context, serviceobj, 'pycsw:Identifier', identifier)
    _set(context, serviceobj, 'pycsw:Typename', 'csw:Record')
    _set(context, serviceobj, 'pycsw:Schema', 'http://www.opengis.net/wfs')
    _set(context, serviceobj, 'pycsw:MdSource', record)
    _set(context, serviceobj, 'pycsw:InsertDate',  get_today_and_now())
    _set(
        context,
        serviceobj,
        'pycsw:AnyText',
        get_anytext(etree.tostring(md._capabilities))
    )
    _set(context, serviceobj, 'pycsw:Type', 'service')
    _set(context, serviceobj, 'pycsw:Title', md.identification.title)
    _set(context, serviceobj, 'pycsw:Abstract', md.identification.abstract)
    _set(context, serviceobj, 'pycsw:Keywords', ','.join(md.identification.keywords))
    _set(context, serviceobj, 'pycsw:Creator', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:Publisher', md.provider.name)
    _set(context, serviceobj, 'pycsw:Contributor', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:OrganizationName', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:AccessConstraints', md.identification.accessconstraints)
    _set(context, serviceobj, 'pycsw:OtherConstraints', md.identification.fees)
    _set(context, serviceobj, 'pycsw:Source', record)
    _set(context, serviceobj, 'pycsw:Format', md.identification.type)
    _set(context, serviceobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
    _set(context, serviceobj, 'pycsw:DistanceUOM', 'degrees')
    _set(context, serviceobj, 'pycsw:ServiceType', 'OGC:WFS')
    _set(context, serviceobj, 'pycsw:ServiceTypeVersion', md.identification.version)
    _set(context, serviceobj, 'pycsw:Operation', ','.join([d.name for d in md.operations]))
    _set(context, serviceobj, 'pycsw:OperatesOn', ','.join(list(md.contents)))
    _set(context, serviceobj, 'pycsw:CouplingType', 'tight')

    links = [{
        'name': identifier,
        'description': 'OGC-WFS Web Feature Service',
        'protocol': 'OGC:WFS',
        'url': md.url
    }]

    _set(context, serviceobj, 'pycsw:Links', json.dumps(links))

    # generate record foreach featuretype

    LOGGER.info('Harvesting %d WFS featuretypes', len(md.contents))

    for featuretype in md.contents:
        recobj = repos.dataset()
        identifier2 = '%s-%s' % (identifier, md.contents[featuretype].id)
        _set(context, recobj, 'pycsw:Identifier', identifier2)
        _set(context, recobj, 'pycsw:Typename', 'csw:Record')
        _set(context, recobj, 'pycsw:Schema', 'http://www.opengis.net/wfs')
        _set(context, recobj, 'pycsw:MdSource', record)
        _set(context, recobj, 'pycsw:InsertDate',  get_today_and_now())
        _set(context, recobj, 'pycsw:Type', 'dataset')
        _set(context, recobj, 'pycsw:ParentIdentifier', identifier)
        _set(context, recobj, 'pycsw:Title', md.contents[featuretype].title)
        _set(context, recobj, 'pycsw:Abstract', md.contents[featuretype].abstract)
        _set(context, recobj, 'pycsw:Keywords', ','.join(md.contents[featuretype].keywords))

        _set(
            context,
            recobj,
            'pycsw:AnyText',
            get_anytext([
                md.contents[featuretype].title,
                md.contents[featuretype].abstract,
                ','.join(md.contents[featuretype].keywords)
            ])
        )

        bbox = md.contents[featuretype].boundingBoxWGS84
        if bbox is not None:
            tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
            wkt_polygon =  bbox2wktpolygon(tmp)
            _set(context, recobj, 'pycsw:BoundingBox', wkt_polygon)
            _set(context, recobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
            _set(context, recobj, 'pycsw:DistanceUOM', 'degrees')
            bboxs.append(wkt_polygon)

        params = {
            'service': 'WFS',
            'version': '1.1.0',
            'request': 'GetFeature',
            'typename': md.contents[featuretype].id,
        }

        links = [{
            'name': md.contents[featuretype].id,
            'description': 'OGC-Web Feature Service',
            'protocol': 'OGC:WFS',
            'url': md.url
            }, {
            'name': md.contents[featuretype].id,
            'description': 'File for download',
            'protocol': 'WWW:DOWNLOAD-1.0-http--download',
            'url': build_get_url(md.url, params)
        }]

        _set(context, recobj, 'pycsw:Links', json.dumps(links))
        _set(context, recobj, 'pycsw:XML', caps2iso(recobj, md, context))
        _set(context, recobj, 'pycsw:MetadataType', 'application/xml')

        recobjs.append(recobj)

    # Derive a bbox based on aggregated featuretype bbox values

    bbox_agg = bbox_from_polygons(bboxs)

    if bbox_agg is not None:
        _set(context, serviceobj, 'pycsw:BoundingBox', bbox_agg)

    _set(context, serviceobj, 'pycsw:XML', caps2iso(serviceobj, md, context))

    recobjs.insert(0, serviceobj)

    return recobjs

def _parse_wcs(context, repos, record, identifier):

    from owslib.wcs import WebCoverageService

    bboxs = []
    recobjs = []
    serviceobj = repos.dataset()

    md = WebCoverageService(record, '1.0.0')

    # generate record of service instance
    _set(context, serviceobj, 'pycsw:Identifier', identifier)
    _set(context, serviceobj, 'pycsw:Typename', 'csw:Record')
    _set(context, serviceobj, 'pycsw:Schema', 'http://www.opengis.net/wcs')
    _set(context, serviceobj, 'pycsw:MdSource', record)
    _set(context, serviceobj, 'pycsw:InsertDate', get_today_and_now())
    _set(
        context,
        serviceobj,
        'pycsw:AnyText',
        get_anytext(etree.tostring(md._capabilities))
    )
    _set(context, serviceobj, 'pycsw:Type', 'service')
    _set(context, serviceobj, 'pycsw:Title', md.identification.title)
    _set(context, serviceobj, 'pycsw:Abstract', md.identification.abstract)
    _set(context, serviceobj, 'pycsw:Keywords', ','.join(md.identification.keywords))
    _set(context, serviceobj, 'pycsw:Creator', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:Publisher', md.provider.name)
    _set(context, serviceobj, 'pycsw:Contributor', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:OrganizationName', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:AccessConstraints', md.identification.accessConstraints)
    _set(context, serviceobj, 'pycsw:OtherConstraints', md.identification.fees)
    _set(context, serviceobj, 'pycsw:Source', record)
    _set(context, serviceobj, 'pycsw:Format', md.identification.type)
    _set(context, serviceobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
    _set(context, serviceobj, 'pycsw:DistanceUOM', 'degrees')
    _set(context, serviceobj, 'pycsw:ServiceType', 'OGC:WCS')
    _set(context, serviceobj, 'pycsw:ServiceTypeVersion', md.identification.version)
    _set(context, serviceobj, 'pycsw:Operation', ','.join([d.name for d in md.operations]))
    _set(context, serviceobj, 'pycsw:OperatesOn', ','.join(list(md.contents)))
    _set(context, serviceobj, 'pycsw:CouplingType', 'tight')

    links = [{
        'name': identifier,
        'description': 'OGC-WCS Web Coverage Service',
        'protocol': 'OGC:WCS',
        'url': md.url
    }]

    _set(context, serviceobj, 'pycsw:Links', json.dumps(links))

    # generate record foreach coverage

    LOGGER.info('Harvesting %d WCS coverages ', len(md.contents))

    for coverage in md.contents:
        recobj = repos.dataset()
        identifier2 = '%s-%s' % (identifier, md.contents[coverage].id)
        _set(context, recobj, 'pycsw:Identifier', identifier2)
        _set(context, recobj, 'pycsw:Typename', 'csw:Record')
        _set(context, recobj, 'pycsw:Schema', 'http://www.opengis.net/wcs')
        _set(context, recobj, 'pycsw:MdSource', record)
        _set(context, recobj, 'pycsw:InsertDate', get_today_and_now())
        _set(context, recobj, 'pycsw:Type', 'dataset')
        _set(context, recobj, 'pycsw:ParentIdentifier', identifier)
        _set(context, recobj, 'pycsw:Title', md.contents[coverage].title)
        _set(context, recobj, 'pycsw:Abstract', md.contents[coverage].abstract)
        _set(context, recobj, 'pycsw:Keywords', ','.join(md.contents[coverage].keywords))

        _set(
            context,
            recobj,
            'pycsw:AnyText',
            get_anytext([
                md.contents[coverage].title,
                md.contents[coverage].abstract,
                ','.join(md.contents[coverage].keywords)
            ])
        )

        bbox = md.contents[coverage].boundingBoxWGS84
        if bbox is not None:
            tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
            wkt_polygon = bbox2wktpolygon(tmp)
            _set(context, recobj, 'pycsw:BoundingBox', wkt_polygon)
            _set(context, recobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
            _set(context, recobj, 'pycsw:DistanceUOM', 'degrees')
            bboxs.append(wkt_polygon)

        links = [{
            'name': md.contents[coverage].id,
            'description': 'OGC-Web Coverage Service',
            'protocol': 'OGC:WCS',
            'url': md.url
        }]

        _set(context, recobj, 'pycsw:Links', json.dumps(links))
        _set(context, recobj, 'pycsw:XML', caps2iso(recobj, md, context))

        recobjs.append(recobj)

    # Derive a bbox based on aggregated coverage bbox values

    bbox_agg = bbox_from_polygons(bboxs)

    if bbox_agg is not None:
        _set(context, serviceobj, 'pycsw:BoundingBox', bbox_agg)

    _set(context, serviceobj, 'pycsw:XML', caps2iso(serviceobj, md, context))
    recobjs.insert(0, serviceobj)

    return recobjs

def _parse_wps(context, repos, record, identifier):

    from owslib.wps import WebProcessingService

    recobjs = []
    serviceobj = repos.dataset()

    md = WebProcessingService(record)

    # generate record of service instance
    _set(context, serviceobj, 'pycsw:Identifier', identifier)
    _set(context, serviceobj, 'pycsw:Typename', 'csw:Record')
    _set(context, serviceobj, 'pycsw:Schema', 'http://www.opengis.net/wps/1.0.0')
    _set(context, serviceobj, 'pycsw:MdSource', record)
    _set(context, serviceobj, 'pycsw:InsertDate', get_today_and_now())
    _set(
        context,
        serviceobj,
        'pycsw:AnyText',
        get_anytext(md._capabilities)
    )
    _set(context, serviceobj, 'pycsw:Type', 'service')
    _set(context, serviceobj, 'pycsw:Title', md.identification.title)
    _set(context, serviceobj, 'pycsw:Abstract', md.identification.abstract)
    _set(context, serviceobj, 'pycsw:Keywords', ','.join(md.identification.keywords))
    _set(context, serviceobj, 'pycsw:Creator', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:Publisher', md.provider.name)
    _set(context, serviceobj, 'pycsw:Contributor', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:OrganizationName', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:AccessConstraints', md.identification.accessconstraints)
    _set(context, serviceobj, 'pycsw:OtherConstraints', md.identification.fees)
    _set(context, serviceobj, 'pycsw:Source', record)
    _set(context, serviceobj, 'pycsw:Format', md.identification.type)

    _set(context, serviceobj, 'pycsw:ServiceType', 'OGC:WPS')
    _set(context, serviceobj, 'pycsw:ServiceTypeVersion', md.identification.version)
    _set(context, serviceobj, 'pycsw:Operation', ','.join([d.name for d in md.operations]))
    _set(context, serviceobj, 'pycsw:OperatesOn', ','.join([o.identifier for o in md.processes]))
    _set(context, serviceobj, 'pycsw:CouplingType', 'loose')

    links = [{
        'name': identifier,
        'description': 'OGC-WPS Web Processing Service',
        'protocol': 'OGC:WPS',
        'url': md.url
        }, {
        'name': identifier,
        'description': 'OGC-WPS Capabilities service (ver 1.0.0)',
        'protocol': 'OGC:WPS-1.1.0-http-get-capabilities',
        'url': build_get_url(md.url, {'service': 'WPS', 'version': '1.0.0', 'request': 'GetCapabilities'})
    }]

    _set(context, serviceobj, 'pycsw:Links', json.dumps(links))
    _set(context, serviceobj, 'pycsw:XML', caps2iso(serviceobj, md, context))
    _set(context, serviceobj, 'pycsw:MetadataType', 'application/xml')

    recobjs.append(serviceobj)

    # generate record foreach process

    LOGGER.info('Harvesting %d WPS processes', len(md.processes))

    for process in md.processes:
        recobj = repos.dataset()
        identifier2 = '%s-%s' % (identifier, process.identifier)
        _set(context, recobj, 'pycsw:Identifier', identifier2)
        _set(context, recobj, 'pycsw:Typename', 'csw:Record')
        _set(context, recobj, 'pycsw:Schema', 'http://www.opengis.net/wps/1.0.0')
        _set(context, recobj, 'pycsw:MdSource', record)
        _set(context, recobj, 'pycsw:InsertDate', get_today_and_now())
        _set(context, recobj, 'pycsw:Type', 'software')
        _set(context, recobj, 'pycsw:ParentIdentifier', identifier)
        _set(context, recobj, 'pycsw:Title', process.title)
        _set(context, recobj, 'pycsw:Abstract', process.abstract)

        _set(
            context,
            recobj,
            'pycsw:AnyText',
            get_anytext([process.title, process.abstract])
        )

        params = {
            'service': 'WPS',
            'version': '1.0.0',
            'request': 'DescribeProcess',
            'identifier': process.identifier
        }

        links.append({
            'name': identifier,
            'description': 'OGC-WPS DescribeProcess service (ver 1.0.0)',
            'protocol': 'OGC:WPS-1.0.0-http-describe-process',
            'url': build_get_url(md.url, {'service': 'WPS', 'version': '1.0.0', 'request': 'DescribeProcess', 'identifier': process.identifier})
        })

        _set(context, recobj, 'pycsw:Links', json.dumps(links))
        _set(context, recobj, 'pycsw:XML', caps2iso(recobj, md, context))
        _set(context, recobj, 'pycsw:MetadataType', 'application/xml')

        recobjs.append(recobj)

    return recobjs


def _parse_sos(context, repos, record, identifier, version):

    from owslib.sos import SensorObservationService

    bboxs = []
    recobjs = []
    serviceobj = repos.dataset()

    if version == '1.0.0':
        schema = 'http://www.opengis.net/sos/1.0'
    else:
        schema = 'http://www.opengis.net/sos/2.0'

    md = SensorObservationService(record, version=version)

    # generate record of service instance
    _set(context, serviceobj, 'pycsw:Identifier', identifier)
    _set(context, serviceobj, 'pycsw:Typename', 'csw:Record')
    _set(context, serviceobj, 'pycsw:Schema', schema)
    _set(context, serviceobj, 'pycsw:MdSource', record)
    _set(context, serviceobj, 'pycsw:InsertDate', get_today_and_now())
    _set(
        context,
        serviceobj,
        'pycsw:AnyText',
        get_anytext(etree.tostring(md._capabilities))
    )
    _set(context, serviceobj, 'pycsw:Type', 'service')
    _set(context, serviceobj, 'pycsw:Title', md.identification.title)
    _set(context, serviceobj, 'pycsw:Abstract', md.identification.abstract)
    _set(context, serviceobj, 'pycsw:Keywords', ','.join(md.identification.keywords))
    _set(context, serviceobj, 'pycsw:Creator', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:Publisher', md.provider.name)
    _set(context, serviceobj, 'pycsw:Contributor', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:OrganizationName', md.provider.contact.name)
    _set(context, serviceobj, 'pycsw:AccessConstraints', md.identification.accessconstraints)
    _set(context, serviceobj, 'pycsw:OtherConstraints', md.identification.fees)
    _set(context, serviceobj, 'pycsw:Source', record)
    _set(context, serviceobj, 'pycsw:Format', md.identification.type)
    _set(context, serviceobj, 'pycsw:CRS', 'urn:ogc:def:crs:EPSG:6.11:4326')
    _set(context, serviceobj, 'pycsw:DistanceUOM', 'degrees')
    _set(context, serviceobj, 'pycsw:ServiceType', 'OGC:SOS')
    _set(context, serviceobj, 'pycsw:ServiceTypeVersion', md.identification.version)
    _set(context, serviceobj, 'pycsw:Operation', ','.join([d.name for d in md.operations]))
    _set(context, serviceobj, 'pycsw:OperatesOn', ','.join(list(md.contents)))
    _set(context, serviceobj, 'pycsw:CouplingType', 'tight')

    links = [{
        'name': identifier,
        'description': 'OGC-SOS Sensor Observation Service',
        'protocol': 'OGC:SOS',
        'url': md.url
    }]

    _set(context, serviceobj, 'pycsw:Links', json.dumps(links))

    # generate record foreach offering

    LOGGER.info('Harvesting %d SOS ObservationOffering\'s ', len(md.contents))

    for offering in md.contents:
        recobj = repos.dataset()
        identifier2 = '%s-%s' % (identifier, md.contents[offering].id)
        _set(context, recobj, 'pycsw:Identifier', identifier2)
        _set(context, recobj, 'pycsw:Typename', 'csw:Record')
        _set(context, recobj, 'pycsw:Schema', schema)
        _set(context, recobj, 'pycsw:MdSource', record)
        _set(context, recobj, 'pycsw:InsertDate', get_today_and_now())
        _set(context, recobj, 'pycsw:Type', 'dataset')
        _set(context, recobj, 'pycsw:ParentIdentifier', identifier)
        _set(context, recobj, 'pycsw:Title', md.contents[offering].description)
        _set(context, recobj, 'pycsw:Abstract', md.contents[offering].description)
        begin = md.contents[offering].begin_position
        end = md.contents[offering].end_position
        _set(context, recobj, 'pycsw:TempExtent_begin',
             datetime2iso8601(begin) if begin is not None else None)
        _set(context, recobj, 'pycsw:TempExtent_end',
             datetime2iso8601(end) if end is not None else None)

        #For observed_properties that have mmi url or urn, we simply want the observation name.
        observed_properties = []
        for obs in md.contents[offering].observed_properties:
          #Observation is stored as urn representation: urn:ogc:def:phenomenon:mmisw.org:cf:sea_water_salinity
          if obs.lower().startswith(('urn:', 'x-urn')):
            observed_properties.append(obs.rsplit(':', 1)[-1])
          #Observation is stored as uri representation: http://mmisw.org/ont/cf/parameter/sea_floor_depth_below_sea_surface
          elif obs.lower().startswith(('http://', 'https://')):
            observed_properties.append(obs.rsplit('/', 1)[-1])
          else:
            observed_properties.append(obs)
        #Build anytext from description and the observed_properties.
        anytext = []
        anytext.append(md.contents[offering].description)
        anytext.extend(observed_properties)
        _set(context, recobj, 'pycsw:AnyText', get_anytext(anytext))
        _set(context, recobj, 'pycsw:Keywords', ','.join(observed_properties))

        bbox = md.contents[offering].bbox
        if bbox is not None:
            tmp = '%s,%s,%s,%s' % (bbox[0], bbox[1], bbox[2], bbox[3])
            wkt_polygon = bbox2wktpolygon(tmp)
            _set(context, recobj, 'pycsw:BoundingBox', wkt_polygon)
            _set(context, recobj, 'pycsw:CRS', md.contents[offering].bbox_srs.id)
            _set(context, recobj, 'pycsw:DistanceUOM', 'degrees')
            bboxs.append(wkt_polygon)

        _set(context, recobj, 'pycsw:XML', caps2iso(recobj, md, context))
        _set(context, recobj, 'pycsw:MetadataType', 'application/xml')
        recobjs.append(recobj)

    # Derive a bbox based on aggregated featuretype bbox values

    bbox_agg = bbox_from_polygons(bboxs)

    if bbox_agg is not None:
        _set(context, serviceobj, 'pycsw:BoundingBox', bbox_agg)

    _set(context, serviceobj, 'pycsw:XML', caps2iso(serviceobj, md, context))
    recobjs.insert(0, serviceobj)

    return recobjs


def _parse_fgdc(context, repos, exml):

    from owslib.fgdc import Metadata

    recobj = repos.dataset()
    links = []

    md = Metadata(exml)

    if md.idinfo.datasetid is not None:  # we need an identifier
        _set(context, recobj, 'pycsw:Identifier', md.idinfo.datasetid)
    else:  # generate one ourselves
        _set(context, recobj, 'pycsw:Identifier', uuid.uuid1().urn)

    _set(context, recobj, 'pycsw:Typename', 'fgdc:metadata')
    _set(context, recobj, 'pycsw:Schema', context.namespaces['fgdc'])
    _set(context, recobj, 'pycsw:MdSource', 'local')
    _set(context, recobj, 'pycsw:InsertDate', get_today_and_now())
    _set(context, recobj, 'pycsw:XML', md.xml)
    _set(context, recobj, 'pycsw:MetadataType', 'application/xml')
    _set(context, recobj, 'pycsw:AnyText', get_anytext(exml))
    _set(context, recobj, 'pycsw:Language', 'en-US')

    if hasattr(md.idinfo, 'descript'):
        _set(context, recobj, 'pycsw:Abstract', md.idinfo.descript.abstract)

    if hasattr(md.idinfo, 'keywords'):
        if md.idinfo.keywords.theme:
            _set(context, recobj, 'pycsw:Keywords', \
            ','.join(md.idinfo.keywords.theme[0]['themekey']))

    if hasattr(md.idinfo.timeperd, 'timeinfo'):
        if hasattr(md.idinfo.timeperd.timeinfo, 'rngdates'):
            _set(context, recobj, 'pycsw:TempExtent_begin',
                 md.idinfo.timeperd.timeinfo.rngdates.begdate)
            _set(context, recobj, 'pycsw:TempExtent_end',
                 md.idinfo.timeperd.timeinfo.rngdates.enddate)

    if hasattr(md.idinfo, 'origin'):
        _set(context, recobj, 'pycsw:Creator', md.idinfo.origin)
        _set(context, recobj, 'pycsw:Publisher',  md.idinfo.origin)
        _set(context, recobj, 'pycsw:Contributor', md.idinfo.origin)

    if hasattr(md.idinfo, 'ptcontac'):
        _set(context, recobj, 'pycsw:OrganizationName', md.idinfo.ptcontac.cntorg)
    _set(context, recobj, 'pycsw:AccessConstraints', md.idinfo.accconst)
    _set(context, recobj, 'pycsw:OtherConstraints', md.idinfo.useconst)
    _set(context, recobj, 'pycsw:Date', md.metainfo.metd)

    if hasattr(md.idinfo, 'spdom') and hasattr(md.idinfo.spdom, 'bbox'):
        bbox = md.idinfo.spdom.bbox
    else:
        bbox = None

    if hasattr(md.idinfo, 'citation'):
        if hasattr(md.idinfo.citation, 'citeinfo'):
            _set(context, recobj, 'pycsw:Type',  md.idinfo.citation.citeinfo['geoform'])
            _set(context, recobj, 'pycsw:Title', md.idinfo.citation.citeinfo['title'])
            _set(context, recobj,
            'pycsw:PublicationDate', md.idinfo.citation.citeinfo['pubdate'])
            _set(context, recobj, 'pycsw:Format', md.idinfo.citation.citeinfo['geoform'])
            if md.idinfo.citation.citeinfo['onlink']:
                for link in md.idinfo.citation.citeinfo['onlink']:
                    links.append({
                        'name': None,
                        'description': None,
                        'protocol': None,
                        'url': link
                    })

    if hasattr(md, 'distinfo') and hasattr(md.distinfo, 'stdorder'):
        for link in md.distinfo.stdorder['digform']:
            tmp = ',%s,,%s' % (link['name'], link['url'])
            links.append({
                'name': None,
                'description': link['name'],
                'protocol': None,
                'url': link['url']
            })

    if len(links) > 0:
        _set(context, recobj, 'pycsw:Links', json.dumps(links))

    if bbox is not None:
        try:
            tmp = '%s,%s,%s,%s' % (bbox.minx, bbox.miny, bbox.maxx, bbox.maxy)
            _set(context, recobj, 'pycsw:BoundingBox', bbox2wktpolygon(tmp))
        except:  # coordinates are corrupted, do not include
            _set(context, recobj, 'pycsw:BoundingBox', None)
    else:
        _set(context, recobj, 'pycsw:BoundingBox', None)

    return recobj

def _parse_gm03(context, repos, exml):

    def get_value_by_language(pt_group, language, pt_type='text'):
        for ptg in pt_group:
            if ptg.language == language:
                if pt_type == 'url':
                    val = ptg.plain_url
                else:  # 'text'
                    val = ptg.plain_text
                return val

    from owslib.gm03 import GM03

    recobj = repos.dataset()
    links = []

    md = GM03(exml)

    if hasattr(md.data, 'core'):
        data = md.data.core
    elif hasattr(md.data, 'comprehensive'):
        data = md.data.comprehensive

    language = data.metadata.language

    _set(context, recobj, 'pycsw:Identifier', data.metadata.file_identifier)
    _set(context, recobj, 'pycsw:Typename', 'gm03:TRANSFER')
    _set(context, recobj, 'pycsw:Schema', context.namespaces['gm03'])
    _set(context, recobj, 'pycsw:MdSource', 'local')
    _set(context, recobj, 'pycsw:InsertDate', get_today_and_now())
    _set(context, recobj, 'pycsw:XML', md.xml)
    _set(context, recobj, 'pycsw:MetadataType', 'application/xml')
    _set(context, recobj, 'pycsw:AnyText', get_anytext(exml))
    _set(context, recobj, 'pycsw:Language', data.metadata.language)
    _set(context, recobj, 'pycsw:Type', data.metadata.hierarchy_level[0])
    _set(context, recobj, 'pycsw:Date', data.metadata.date_stamp)

    for dt in data.date:
        if dt.date_type == 'modified':
            _set(context, recobj, 'pycsw:Modified', dt.date)
        elif dt.date_type == 'creation':
            _set(context, recobj, 'pycsw:CreationDate', dt.date)
        elif dt.date_type == 'publication':
            _set(context, recobj, 'pycsw:PublicationDate', dt.date)
        elif dt.date_type == 'revision':
            _set(context, recobj, 'pycsw:RevisionDate', dt.date)

    if hasattr(data, 'metadata_point_of_contact'):
        _set(context, recobj, 'pycsw:ResponsiblePartyRole', data.metadata_point_of_contact.role)

    _set(context, recobj, 'pycsw:Source', data.metadata.dataset_uri)
    _set(context, recobj, 'pycsw:CRS','urn:ogc:def:crs:EPSG:6.11:4326')

    if hasattr(data, 'citation'):
        _set(context, recobj, 'pycsw:Title', get_value_by_language(data.citation.title.pt_group, language))

    if hasattr(data, 'data_identification'):
        _set(context, recobj, 'pycsw:Abstract', get_value_by_language(data.data_identification.abstract.pt_group, language))
        if hasattr(data.data_identification, 'topic_category'):
            _set(context, recobj, 'pycsw:TopicCategory', data.data_identification.topic_category)
        _set(context, recobj, 'pycsw:ResourceLanguage', data.data_identification.language)

    if hasattr(data, 'format'):
        _set(context, recobj, 'pycsw:Format', data.format.name)

    # bbox
    if hasattr(data, 'geographic_bounding_box'):
        try:
            tmp = '%s,%s,%s,%s' % (data.geographic_bounding_box.west_bound_longitude,
                                   data.geographic_bounding_box.south_bound_latitude,
                                   data.geographic_bounding_box.east_bound_longitude,
                                   data.geographic_bounding_box.north_bound_latitude)
            _set(context, recobj, 'pycsw:BoundingBox', bbox2wktpolygon(tmp))
        except:  # coordinates are corrupted, do not include
            _set(context, recobj, 'pycsw:BoundingBox', None)
    else:
        _set(context, recobj, 'pycsw:BoundingBox', None)

    # temporal extent
    if hasattr(data, 'temporal_extent'):
        if data.temporal_extent.extent['begin'] is not None and data.temporal_extent.extent['end'] is not None:
            _set(context, recobj, 'pycsw:TempExtent_begin', data.temporal_extent.extent['begin'])
            _set(context, recobj, 'pycsw:TempExtent_end', data.temporal_extent.extent['end'])

    # online linkages
    name = description = protocol = ''

    if hasattr(data, 'online_resource'):
        if hasattr(data.online_resource, 'name'):
            name = get_value_by_language(data.online_resource.name.pt_group, language)
        if hasattr(data.online_resource, 'description'):
            description = get_value_by_language(data.online_resource.description.pt_group, language)
        linkage = get_value_by_language(data.online_resource.linkage.pt_group, language, 'url')

        links.append({
            'name': name,
            'description': description,
            'protocol': protocol,
            'url': linkage
        })

    if len(links) > 0:
        _set(context, recobj, 'pycsw:Links', json.dumps(links))

    keywords = []
    for kw in data.keywords:
        keywords.append(get_value_by_language(kw.keyword.pt_group, language))
        _set(context, recobj, 'pycsw:Keywords', ','.join(keywords))

    # contacts
    return recobj

def _parse_iso(context, repos, exml):

    from owslib.iso import MD_Metadata
    from owslib.iso_che import CHE_MD_Metadata

    recobj = repos.dataset()
    links = []

    if exml.tag == '{http://www.geocat.ch/2008/che}CHE_MD_Metadata':
        md = CHE_MD_Metadata(exml)
    else:
        md = MD_Metadata(exml)

    _set(context, recobj, 'pycsw:Identifier', md.identifier)
    _set(context, recobj, 'pycsw:Typename', 'gmd:MD_Metadata')
    _set(context, recobj, 'pycsw:Schema', context.namespaces['gmd'])
    _set(context, recobj, 'pycsw:MdSource', 'local')
    _set(context, recobj, 'pycsw:InsertDate', get_today_and_now())
    _set(context, recobj, 'pycsw:XML', md.xml)
    _set(context, recobj, 'pycsw:MetadataType', 'application/xml')
    _set(context, recobj, 'pycsw:AnyText', get_anytext(exml))
    _set(context, recobj, 'pycsw:Language', md.language)
    _set(context, recobj, 'pycsw:Type', md.hierarchy)
    _set(context, recobj, 'pycsw:ParentIdentifier', md.parentidentifier)
    _set(context, recobj, 'pycsw:Date', md.datestamp)
    _set(context, recobj, 'pycsw:Modified', md.datestamp)
    _set(context, recobj, 'pycsw:Source', md.dataseturi)
    if md.referencesystem is not None:
        try:
            code_ = 'urn:ogc:def:crs:EPSG::%d' % int(md.referencesystem.code)
        except ValueError:
            code_ = md.referencesystem.code
        _set(context, recobj, 'pycsw:CRS', code_)

    if hasattr(md, 'identification'):
        _set(context, recobj, 'pycsw:Title', md.identification.title)
        _set(context, recobj, 'pycsw:AlternateTitle', md.identification.alternatetitle)
        _set(context, recobj, 'pycsw:Abstract', md.identification.abstract)
        _set(context, recobj, 'pycsw:Relation', md.identification.aggregationinfo)

        if hasattr(md.identification, 'temporalextent_start'):
            _set(context, recobj, 'pycsw:TempExtent_begin', md.identification.temporalextent_start)
        if hasattr(md.identification, 'temporalextent_end'):
            _set(context, recobj, 'pycsw:TempExtent_end', md.identification.temporalextent_end)

        if len(md.identification.topiccategory) > 0:
            _set(context, recobj, 'pycsw:TopicCategory', md.identification.topiccategory[0])

        if len(md.identification.resourcelanguage) > 0:
            _set(context, recobj, 'pycsw:ResourceLanguage', md.identification.resourcelanguage[0])

        if hasattr(md.identification, 'bbox'):
            bbox = md.identification.bbox
        else:
            bbox = None

        if (hasattr(md.identification, 'keywords') and
            len(md.identification.keywords) > 0):
            all_keywords = [item for sublist in md.identification.keywords for item in sublist['keywords'] if item is not None]
            _set(context, recobj, 'pycsw:Keywords', ','.join(all_keywords))
            _set(context, recobj, 'pycsw:KeywordType', md.identification.keywords[0]['type'])

        if (hasattr(md.identification, 'creator') and
            len(md.identification.creator) > 0):
            all_orgs = set([item.organization for item in md.identification.creator if hasattr(item, 'organization') and item.organization is not None])
            _set(context, recobj, 'pycsw:Creator', ';'.join(all_orgs))
        if (hasattr(md.identification, 'publisher') and
            len(md.identification.publisher) > 0):
            all_orgs = set([item.organization for item in md.identification.publisher if hasattr(item, 'organization') and item.organization is not None])
            _set(context, recobj, 'pycsw:Publisher', ';'.join(all_orgs))
        if (hasattr(md.identification, 'contributor') and
            len(md.identification.contributor) > 0):
            all_orgs = set([item.organization for item in md.identification.contributor if hasattr(item, 'organization') and item.organization is not None])
            _set(context, recobj, 'pycsw:Contributor', ';'.join(all_orgs))

        if (hasattr(md.identification, 'contact') and
            len(md.identification.contact) > 0):
            all_orgs = set([item.organization for item in md.identification.contact if hasattr(item, 'organization') and item.organization is not None])
            _set(context, recobj, 'pycsw:OrganizationName', ';'.join(all_orgs))

        if len(md.identification.securityconstraints) > 0:
            _set(context, recobj, 'pycsw:SecurityConstraints',
            md.identification.securityconstraints[0])
        if len(md.identification.accessconstraints) > 0:
            _set(context, recobj, 'pycsw:AccessConstraints',
            md.identification.accessconstraints[0])
        if len(md.identification.otherconstraints) > 0:
            _set(context, recobj, 'pycsw:OtherConstraints', md.identification.otherconstraints[0])

        if hasattr(md.identification, 'date'):
            for datenode in md.identification.date:
                if datenode.type == 'revision':
                    _set(context, recobj, 'pycsw:RevisionDate', datenode.date)
                elif datenode.type == 'creation':
                    _set(context, recobj, 'pycsw:CreationDate', datenode.date)
                elif datenode.type == 'publication':
                    _set(context, recobj, 'pycsw:PublicationDate', datenode.date)

        if hasattr(md.identification, 'extent') and hasattr(md.identification.extent, 'description_code'):
            _set(context, recobj, 'pycsw:GeographicDescriptionCode', md.identification.extent.description_code)

        if len(md.identification.denominators) > 0:
            _set(context, recobj, 'pycsw:Denominator', md.identification.denominators[0])
        if len(md.identification.distance) > 0:
            _set(context, recobj, 'pycsw:DistanceValue', md.identification.distance[0])
        if len(md.identification.uom) > 0:
            _set(context, recobj, 'pycsw:DistanceUOM', md.identification.uom[0])

        if len(md.identification.classification) > 0:
            _set(context, recobj, 'pycsw:Classification', md.identification.classification[0])
        if len(md.identification.uselimitation) > 0:
            _set(context, recobj, 'pycsw:ConditionApplyingToAccessAndUse',
            md.identification.uselimitation[0])

    if hasattr(md.identification, 'format'):
        _set(context, recobj, 'pycsw:Format', md.distribution.format)

    if md.serviceidentification is not None:
        _set(context, recobj, 'pycsw:ServiceType', md.serviceidentification.type)
        _set(context, recobj, 'pycsw:ServiceTypeVersion', md.serviceidentification.version)

        _set(context, recobj, 'pycsw:CouplingType', md.serviceidentification.couplingtype)

    service_types = []
    for smd in md.identificationinfo:
        if smd.identtype == 'service' and smd.type is not None:
            service_types.append(smd.type)

    _set(context, recobj, 'pycsw:ServiceType', ','.join(service_types))

        #if len(md.serviceidentification.operateson) > 0:
        #    _set(context, recobj, 'pycsw:operateson = VARCHAR(32),
        #_set(context, recobj, 'pycsw:operation VARCHAR(32),
        #_set(context, recobj, 'pycsw:operatesonidentifier VARCHAR(32),
        #_set(context, recobj, 'pycsw:operatesoname VARCHAR(32),


    if hasattr(md.identification, 'dataquality'):
        _set(context, recobj, 'pycsw:Degree', md.dataquality.conformancedegree)
        _set(context, recobj, 'pycsw:Lineage', md.dataquality.lineage)
        _set(context, recobj, 'pycsw:SpecificationTitle', md.dataquality.specificationtitle)
        if hasattr(md.dataquality, 'specificationdate'):
            _set(context, recobj, 'pycsw:specificationDate',
            md.dataquality.specificationdate[0].date)
            _set(context, recobj, 'pycsw:SpecificationDateType',
            md.dataquality.specificationdate[0].datetype)

    if hasattr(md, 'contact') and len(md.contact) > 0:
        _set(context, recobj, 'pycsw:ResponsiblePartyRole', md.contact[0].role)


    if hasattr(md, 'contentinfo') and len(md.contentinfo) > 0:
        for ci in md.contentinfo:
            if isinstance(ci, MD_ImageDescription):
                _set(context, recobj, 'pycsw:CloudCover', ci.cloud_cover)

                keywords = _get(context, recobj, 'pycsw:Keywords')
                if ci.processing_level is not None:
                    pl_keyword = 'eo:processingLevel:' + ci.processing_level
                    if keywords is None:
                        keywords  = pl_keyword
                    else:
                        keywords  += ',' + pl_keyword

                    _set(context, recobj, 'pycsw:Keywords', keywords)

            bands = []
            if hasattr(ci, 'bands'):
                for band in ci.bands:
                    band_info = {
                        'id': band.id,
                        'units': band.units,
                        'min': band.min,
                        'max': band.max
                    }
                    bands.append(band_info)

            if len(bands) > 0:
                _set(context, recobj, 'pycsw:Bands', json.dumps(bands))

    if hasattr(md, 'acquisition') and md.acquisition is not None:
        platform = md.acquisition.platforms[0]
        _set(context, recobj, 'pycsw:Platform', platform.identifier)

        instrument = platform.instruments[0]
        _set(context, recobj, 'pycsw:Instrument', instrument.identifier)
        _set(context, recobj, 'pycsw:SensorType', instrument.type)

    LOGGER.info('Scanning for links')
    if hasattr(md, 'distribution'):
        dist_links = []
        if hasattr(md.distribution, 'online'):
            LOGGER.debug('Scanning for gmd:transferOptions element(s)')
            dist_links.extend(md.distribution.online)
        if hasattr(md.distribution, 'distributor'):
            LOGGER.debug('Scanning for gmd:distributorTransferOptions element(s)')
            for dist_member in md.distribution.distributor:
                dist_links.extend(dist_member.online)
        for link in dist_links:
            if link.url is not None and link.protocol is None:  # take a best guess
                link.protocol = sniff_link(link.url)
            links.append({
                'name': link.name,
                'description': link.description,
                'protocol': link.protocol,
                'url': link.url
            })

    try:
        LOGGER.debug('Scanning for srv:SV_ServiceIdentification links')
        for sident in md.identificationinfo:
            if hasattr(sident, 'operations'):
                for sops in sident.operations:
                    for scpt in sops['connectpoint']:
                        LOGGER.debug('adding srv link %s', scpt.url)
                        linkobj = {
                            'name': scpt.name,
                            'description': scpt.description,
                            'protocol': scpt.protocol,
                            'url': scpt.url
                        }
                        links.append(linkobj)
    except Exception as err:  # srv: identification does not exist
        LOGGER.exception('no srv:SV_ServiceIdentification links found')

    if len(links) > 0:
        _set(context, recobj, 'pycsw:Links', json.dumps(links))

    if bbox is not None:
        try:
            tmp = '%s,%s,%s,%s' % (bbox.minx, bbox.miny, bbox.maxx, bbox.maxy)
            _set(context, recobj, 'pycsw:BoundingBox', bbox2wktpolygon(tmp))
        except:  # coordinates are corrupted, do not include
            _set(context, recobj, 'pycsw:BoundingBox', None)
    else:
        _set(context, recobj, 'pycsw:BoundingBox', None)

    return recobj

def _parse_dc(context, repos, exml):

    from owslib.csw import CswRecord

    recobj = repos.dataset()
    links = []

    md = CswRecord(exml)

    if md.bbox is None:
        bbox = None
    else:
        bbox = md.bbox

    _set(context, recobj, 'pycsw:Identifier', md.identifier)
    _set(context, recobj, 'pycsw:Typename', 'csw:Record')
    _set(context, recobj, 'pycsw:Schema', context.namespaces['csw'])
    _set(context, recobj, 'pycsw:MdSource', 'local')
    _set(context, recobj, 'pycsw:InsertDate',  get_today_and_now())
    _set(context, recobj, 'pycsw:XML', md.xml)
    _set(context, recobj, 'pycsw:MetadataType', 'application/xml')
    _set(context, recobj, 'pycsw:AnyText',  get_anytext(exml))
    _set(context, recobj, 'pycsw:Language', md.language)
    _set(context, recobj, 'pycsw:Type', md.type)
    _set(context, recobj, 'pycsw:Title', md.title)
    _set(context, recobj, 'pycsw:AlternateTitle', md.alternative)
    _set(context, recobj, 'pycsw:Abstract', md.abstract)

    if len(md.subjects) > 0 and None not in md.subjects:
        _set(context, recobj, 'pycsw:Keywords', ','.join(md.subjects))

    _set(context, recobj, 'pycsw:ParentIdentifier', md.ispartof)
    _set(context, recobj, 'pycsw:Relation', md.relation)
    _set(context, recobj, 'pycsw:TempExtent_begin', md.temporal)
    _set(context, recobj, 'pycsw:TempExtent_end', md.temporal)
    _set(context, recobj, 'pycsw:ResourceLanguage', md.language)
    _set(context, recobj, 'pycsw:Creator', md.creator)
    _set(context, recobj, 'pycsw:Publisher', md.publisher)
    _set(context, recobj, 'pycsw:Contributor', md.contributor)
    _set(context, recobj, 'pycsw:OrganizationName', md.rightsholder)
    _set(context, recobj, 'pycsw:AccessConstraints', md.accessrights)
    _set(context, recobj, 'pycsw:OtherConstraints', md.license)
    _set(context, recobj, 'pycsw:Date', md.date)
    _set(context, recobj, 'pycsw:CreationDate', md.created)
    _set(context, recobj, 'pycsw:PublicationDate', md.issued)
    _set(context, recobj, 'pycsw:Modified', md.modified)
    _set(context, recobj, 'pycsw:Format', md.format)
    _set(context, recobj, 'pycsw:Source', md.source)

    for ref in md.references:
        links.append({
            'name': None,
            'description': None,
            'procotcol': ref['scheme'],
            'url': ref['url'],
        })
    for uri in md.uris:
        links.append({
            'name': uri['name'],
            'description': uri['description'],
            'procotcol': uri['protocol'],
            'url': uri['url'],
        })

    if len(links) > 0:
        _set(context, recobj, 'pycsw:Links', json.dumps(links))

    if bbox is not None:
        try:
            tmp = '%s,%s,%s,%s' % (bbox.minx, bbox.miny, bbox.maxx, bbox.maxy)
            _set(context, recobj, 'pycsw:BoundingBox',  bbox2wktpolygon(tmp))
        except:  # coordinates are corrupted, do not include
            _set(context, recobj, 'pycsw:BoundingBox', None)
    else:
        _set(context, recobj, 'pycsw:BoundingBox', None)

    return recobj


def caps2iso(record, caps, context):
    """Creates ISO metadata from Capabilities XML"""

    from pycsw.plugins.profiles.apiso.apiso import APISO

    apiso_obj = APISO(context.model, context.namespaces, context)
    apiso_obj.ogc_schemas_base = 'http://schemas.opengis.net'
    apiso_obj.url = context.url
    queryables = dict(apiso_obj.repository['queryables']['SupportedISOQueryables'].items())
    iso_xml = apiso_obj.write_record(record, 'full', 'http://www.isotc211.org/2005/gmd', queryables, caps)
    return etree.tostring(iso_xml)


def bbox_from_polygons(bboxs):
    """Derive an aggregated bbox from n polygons
    Parameters
    ----------
    bboxs: list
        A sequence of strings containing Well-Known Text representations of
        polygons
    Returns
    -------
    str
        Well-Known Text representation of the aggregated bounding box for
        all the input polygons
    """

    try:
        multi_pol = MultiPolygon(
            [loads(bbox) for bbox in bboxs]
        )
        bstr = ",".join(["{0:.2f}".format(b) for b in multi_pol.bounds])
        return bbox2wktpolygon(bstr)
    except Exception as err:
        raise RuntimeError('Cannot aggregate polygons: %s' % str(err))

def datetime2iso8601(value):
    """Return a datetime value as ISO8601
    Parameters
    ----------
    value: datetime.date or datetime.datetime
        The temporal value to be converted
    Returns
    -------
    str
        A string with the temporal value in ISO8601 format.
    """

    if isinstance(value, datetime.datetime):
        if value == value.replace(hour=0, minute=0, second=0, microsecond=0):
            result = value.strftime("%Y-%m-%d")
        else:
            result = value.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:  # value is a datetime.date
        result = value.strftime('%Y-%m-%d')
    return result


def get_time_iso2unix(isotime):
    """Convert ISO8601 to UNIX timestamp"""
    return int(time.mktime(time.strptime(
        isotime, '%Y-%m-%dT%H:%M:%SZ'))) - time.timezone


def get_version_integer(version):
    """Get an integer of the OGC version value x.y.z
    In case of an invalid version string this returns -1.
    Parameters
    ----------
    version: str
        The version string that is to be transformed into an integer
    Returns
    -------
    int
        The transformed version
    Raises
    ------
    RuntimeError
        When the input version is neither a string or None
    """

    try:
        xyz = version.split('.')
        if len(xyz) == 3:
            result = int(xyz[0]) * 10000 + int(xyz[1]) * 100 + int(xyz[2])
        else:
            result = -1
    except AttributeError as err:
        raise RuntimeError('%s' % str(err))
    return result


def nspath_eval(xpath, nsmap):
    """Return an etree friendly xpath.
    This function converts XPath expressions that use prefixes into
    their full namespace. This is the form expected by lxml [1]_.
    Parameters
    ----------
    xpath: str
        The XPath expression to be converted
    nsmap: dict
    Returns
    -------
    str
        The XPath expression using namespaces instead of prefixes.
    References
    ----------
    .. [1] http://lxml.de/tutorial.html#namespaces
    """

    out = []
    for node in xpath.split('/'):
        chunks = node.split(":")
        if len(chunks) == 2:
            prefix, element = node.split(':')
            out.append('{%s}%s' % (nsmap[prefix], element))
        elif len(chunks) == 1:
            out.append(node)
        else:
            raise RuntimeError("Invalid XPath expression: {0}".format(xpath))
    return '/'.join(out)


def wktenvelope2bbox(envelope):
    """returns bbox string of WKT ENVELOPE definition"""

    tmparr = [x.strip() for x in envelope.split('(')[1].split(')')[0].split(',')]
    bbox = '%s,%s,%s,%s' % (tmparr[0], tmparr[3], tmparr[1], tmparr[2])
    return bbox


def wkt2geom(ewkt, bounds=True):
    """Return Shapely geometry object based on WKT/EWKT
    Parameters
    ----------
    ewkt: str
        The geometry to convert, in Extended Well-Known Text format. More info
        on this format at [1]_
    bounds: bool
        Whether to return only the bounding box of the geometry as a tuple or
        the full shapely geometry instance
    Returns
    -------
    shapely.geometry.base.BaseGeometry or tuple
        Depending on the value of the ``bounds`` parameter, returns either 
        the shapely geometry instance or a tuple with the bounding box.
    References
    ----------
    .. [1] http://postgis.net/docs/ST_GeomFromEWKT.html
    """

    wkt = ewkt.split(";")[-1] if ewkt.find("SRID") != -1 else ewkt
    if wkt.startswith('ENVELOPE'):
        wkt = bbox2wktpolygon(wktenvelope2bbox(wkt))
    geometry = loads(wkt)
    return geometry.envelope.bounds if bounds else geometry


def bbox2wktpolygon(bbox):
    """Return OGC WKT Polygon of a simple bbox string
    Parameters
    ----------
    bbox: str
        The bounding box to convert to WKT.
    Returns
    -------
    str
        The bounding box's Well-Known Text representation.
    """

    if bbox.startswith('ENVELOPE'):
        bbox = wktenvelope2bbox(bbox)
    minx, miny, maxx, maxy = [float(coord) for coord in bbox.split(",")]
    return 'POLYGON((%.2f %.2f, %.2f %.2f, %.2f %.2f, %.2f %.2f, %.2f %.2f))' \
        % (minx, miny, minx, maxy, maxx, maxy, maxx, miny, minx, miny)


def transform_mappings(queryables, typename):
    """Transform metadata model mappings
    Parameters
    ----------
    queryables: dict
    typename: dict
    """

    for item in queryables:
        try:
            matching_typename = [key for key, value in typename.items() if
                                 value == item][0]
            queryable_value = queryables[matching_typename]
            queryables[item] = {
                "xpath": queryable_value["xpath"],
                "dbcol": queryable_value["dbcol"],
            }
        except IndexError:
            pass


def getqattr(obj, name):
    """Get value of an object, safely"""
    result = None
    try:
        item = getattr(obj, name)
        value = item()
        if "link" in name:  # create link format
            links = []
            for link in value:
                links.append(','.join(list(link)))
            result = '^'.join(links)
        else:
            result = value
    except TypeError:  # item is not callable
        try:
            result = datetime2iso8601(item)
        except AttributeError:  # item is not date(time)
            result = item
    except AttributeError:  # obj does not have a name property
        pass
    return result


def http_request(method, url, request=None, timeout=30):
    """Perform HTTP request"""
    if method == 'POST':
        return http_post(url, request, timeout=timeout)
    else:  # GET
        request = Request(url)
        request.add_header('User-Agent', 'pycsw (https://pycsw.org/)')
        return urlopen(request, timeout=timeout).read()


def bind_url(url):
    """binds an HTTP GET query string endpoint"""
    parsed_url = urlparse(url)
    if parsed_url.query == "":
        binder = "?"
    elif parsed_url.query.endswith("&"):
        binder = ""
    else:
        binder = "&"
    return "".join((parsed_url.geturl(), binder))


def ip_in_network_cidr(ip, net):
    """decipher whether IP is within CIDR range"""
    ipaddr = int(
        ''.join(['%02x' % int(x) for x in ip.split('.')]),
        16
    )
    netstr, bits = net.split('/')
    netaddr = int(
        ''.join(['%02x' % int(x) for x in netstr.split('.')]),
        16
    )
    mask = (0xffffffff << (32 - int(bits))) & 0xffffffff
    return (ipaddr & mask) == (netaddr & mask)


def ipaddress_in_whitelist(ipaddress, whitelist):
    """decipher whether IP is in IP whitelist
    IP whitelist is a list supporting:
    - single IP address (e.g. 192.168.0.1)
    - IP range using CIDR (e.g. 192.168.0/22)
    - IP range using subnet wildcard (e.g. 192.168.0.*, 192.168.*)
    """

    if ipaddress in whitelist:
        return True
    else:
        for white in whitelist:
            if white.find('/') != -1:  # CIDR
                if ip_in_network_cidr(ipaddress, white):
                    return True
            elif white.find('*') != -1:  # subnet wildcard
                    if ipaddress.startswith(white.split('*')[0]):
                        return True
    return False


def get_anytext(bag):
    """
    generate bag of text for free text searches
    accepts list of words, string of XML, or etree.Element
    """

    if isinstance(bag, list):  # list of words
        return ' '.join([_f for _f in bag if _f]).strip()
    else:  # xml
        if isinstance(bag, bytes) or isinstance(bag, str):
            # serialize to lxml
            bag = etree.fromstring(bag, PARSER)
        # get all XML element content
        return ' '.join([value.strip() for value in bag.xpath('//text()')])


# https://github.com/pallets/werkzeug/blob/778f482d1ac0c9e8e98f774d2595e9074e6984d7/werkzeug/utils.py#L253
def secure_filename(filename):
    r"""Pass it a filename and it will return a secure version of it.  This
    filename can then safely be stored on a regular file system and passed
    to :func:`os.path.join`.  The filename returned is an ASCII only string
    for maximum portability.
    On windows systems the function also makes sure that the file is not
    named after one of the special device files.
    >>> secure_filename("My cool movie.mov")
    'My_cool_movie.mov'
    >>> secure_filename("../../../etc/passwd")
    'etc_passwd'
    >>> secure_filename(u'i contain cool \xfcml\xe4uts.txt')
    'i_contain_cool_umlauts.txt'
    The function might return an empty filename.  It's your responsibility
    to ensure that the filename is unique and that you abort or
    generate a random filename if the function returned an empty one.
    .. versionadded:: 0.5
    :param filename: the filename to secure
    """
    if isinstance(filename, str):
        from unicodedata import normalize
        filename = normalize('NFKD', filename).encode('ascii', 'ignore')
        filename = filename.decode('ascii')
    for sep in os.path.sep, os.path.altsep:
        if sep:
            filename = filename.replace(sep, ' ')
    filename = str(_filename_ascii_strip_re.sub('', '_'.join(
                   filename.split()))).strip('._')

    # on nt a couple of special files are present in each folder.  We
    # have to ensure that the target file is not such a filename.  In
    # this case we prepend an underline
    if os.name == 'nt' and filename and \
       filename.split('.')[0].upper() in _windows_device_files:
        filename = '_' + filename

    return filename

def jsonify_links(links):
    """
    pycsw:Links column data handler.
    casts old or new style links into JSON objects
    """
    try:
        LOGGER.debug('JSON link')
        linkset = json.loads(links)
        return linkset
    except json.decoder.JSONDecodeError as err:  # try CSV parsing
        LOGGER.debug('old style CSV link')
        json_links = []
        for link in links.split('^'):
            tokens = link.split(',')
            json_links.append({
                'name': tokens[0] or None,
                'description': tokens[1] or None,
                'protocol': tokens[2] or None,
                'url': tokens[3] or None
            })
        return json_links

def get_csw_records(csw, pagesize=10, maxrecords=1000):
    """Iterate `maxrecords`/`pagesize` times until the requested value in
    `maxrecords` is reached.
    """
    from owslib.fes import SortBy, SortProperty

    # Iterate over sorted results.
    sortby = SortBy([SortProperty("dc:title", "ASC")])
    csw_records = {}
    startposition = 0
    nextrecord = getattr(csw, "results", 1)
    while nextrecord != 0:
        csw.getrecords2(
            #constraints=filter_list,
            startposition=startposition,
            maxrecords=pagesize,
            sortby=sortby,
        )
        csw_records.update(csw.records)
        if csw.results["nextrecord"] == 0:
            break
        startposition += pagesize + 1  # Last one is included.
        if startposition >= maxrecords:
            break
    csw.records.update(csw_records)

def save_new_metadata(filename, username, statusMetadata=False):
    metadata = Metadata.query.filter_by(filename=filename).first()
    print(filename, ' ' , username, ' ', statusMetadata)
    print(metadata)
    if not metadata:
        if validate_xml(os.path.join(UPLOAD_FOLDER,filename)) == 'Valid':
            try:            
                user = User.query.filter_by(username=username).first()
                if not user:
                    response_object = {
                        'status': 'fail',
                        'message': 'Username not found',
                    }
                    return response_object, 200
                else:

                    #base = 'http://localhost:7001/csw'
                    #csw = CatalogueServiceWeb(base, timeout=30)
                    if statusMetadata :
                        CP = configparser.ConfigParser(interpolation=EnvInterpolation())
                        CFG = os.path.join(os.getcwd(), 'pycsw.cfg')
                        with open(CFG) as f:
                            CP.read_file(f)
                        DATABASE = CP.get('repository', 'database')
                        try:
                            TABLE = CP.get('repository', 'table')
                        except configparser.NoOptionError:
                            TABLE = 'records2'
                        
                        CONTEXT = StaticContext()

                        try:
                            print(DATABASE, ' ' , TABLE)
                            exml = etree.parse(os.path.join(UPLOAD_FOLDER,filename), CONTEXT.parser)
                        except etree.XMLSyntaxError as err:
                            print('XML document "%s" is not well-formed')
                            #continue
                        except Exception as err:
                            print('XML document "%s" is not well-formed')

                        REPO = Repository(DATABASE, CONTEXT, TABLE)

                        record = parse_record(CONTEXT, exml, REPO)
                        #get user
                        for rec in record:
                            print(rec.identifier)
                            print(rec.typename)
                            print(rec.schema)
                            print(rec.mdsource)
                            print(rec.insert_date)
                            REPO.insert(rec, 'local', get_today_and_now())

                    new_metadata = Metadata(
                        filename=filename,
                        time_uploaded=datetime.datetime.utcnow(),
                        user_id = user.id,
                        validated = True,
                        status = statusMetadata
                    )
                    save_changes(new_metadata)
                    response_object = {
                        'status': 'success',
                        'message': 'Successfully inserted.'
                    }
                    return response_object, 201
            except Exception as err:
                print('Could not parse "%s" as an XML record')
                print(err)
                        
            
        else:
            response_object = {
                'status': 'fail',
                'message': 'XML is not valid.'
            }
            return response_object, 200
    else:
        response_object = {
            'status': 'fail',
            'message': 'Metadata filename already exists',
        }
        return response_object, 200

def get_a_metadata(id):
    return Metadata.query.filter_by(id=id).first()
    # 
def get_all():
    return Metadata.query.order_by('filename').all()


def get_all_username(user):
    user = User.query.filter_by(username=user).first()
    #print(user.__dict__)
    if user:
        return Metadata.query.filter_by(user_id=user.id).order_by('time_uploaded').all()
    else:
        response_object = {
            'status': 'fail',
            'message': 'User not found',
        }
        return response_object, 200

def save_changes(data):
    db.session.add(data)
    db.session.commit()

def update_metadata(data):
    metadata = Metadata.query.filter_by(id=data['id']).first()
    if metadata:
        setattr(metadata, 'filename', data['filename'])
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully updated.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Organization not found',
        }
        return response_object, 200

def update_metadata_admin(data):
    metadata = Metadata.query.filter_by(id=data['id']).first()
    if metadata:
        CP = configparser.ConfigParser(interpolation=EnvInterpolation())
        CFG = os.path.join(os.getcwd(), 'pycsw.cfg')
        with open(CFG) as f:
            CP.read_file(f)
        DATABASE = CP.get('repository', 'database')
        try:
            TABLE = CP.get('repository', 'table')
        except configparser.NoOptionError:
            TABLE = 'records2'
        
        CONTEXT = StaticContext()

        try:
            print(DATABASE, ' ' , TABLE)
            exml = etree.parse(os.path.join(UPLOAD_FOLDER,metadata.filename), CONTEXT.parser)
        except etree.XMLSyntaxError as err:
            print('XML document "%s" is not well-formed')
            #continue
        except Exception as err:
            print('Exception error "%s" is not well-formed')

        REPO = Repository(DATABASE, CONTEXT, TABLE)

        record = parse_record(CONTEXT, exml, REPO)
        if data['statusMetadata'] == 'true' :
            statusMetadata = True
            #get user
            for rec in record:
                print(rec.identifier)
                REPO.insert(rec, 'local', get_today_and_now())
        else:
            statusMetadata = False
            for rec in record:
                print(rec.identifier + ' delete')
                REPO.delete({'where': 'identifier=:pvalue0', 'values': [rec.identifier]})

        setattr(metadata, 'status', statusMetadata)
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully updated.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Organization not found',
        }
        return response_object, 200

def delete_metadata(data):
    metadata = Metadata.query.filter_by(id=data['id']).first()
    if metadata:
        CP = configparser.ConfigParser(interpolation=EnvInterpolation())
        CFG = os.path.join(os.getcwd(), 'pycsw.cfg')
        with open(CFG) as f:
            CP.read_file(f)
        DATABASE = CP.get('repository', 'database')
        try:
            TABLE = CP.get('repository', 'table')
        except configparser.NoOptionError:
            TABLE = 'records2'
        
        CONTEXT = StaticContext()
        #print(metadata.filename)
        try:
            print(DATABASE, ' ' , TABLE)
            exml = etree.parse(os.path.join(UPLOAD_FOLDER,metadata.filename), CONTEXT.parser)
            REPO = Repository(DATABASE, CONTEXT, TABLE)
            record = parse_record(CONTEXT, exml, REPO)
            for rec in record:
                print(rec.identifier + ' dsadsa')
                REPO.delete({'where': 'identifier=:pvalue0', 'values': [rec.identifier]})
        except etree.XMLSyntaxError as err:
            print('XML document "%s" is not well-formed')
            #continue
        except Exception as err:
            print('err "%s" is not well-formed', err)

        
        Metadata.query.filter_by(id=data['id']).delete()
        db.session.commit()
        response_object = {
            'status': 'success',
            'message': 'Successfully deleted.'
        }
        return response_object, 201
    else:
        response_object = {
            'status': 'fail',
            'message': 'Metadata not found',
        }
        return response_object, 200


def get_info(id):
    gallery = Metadata.query.filter_by(id=id).first()
    if gallery:
        user = User.query.filter_by(id=gallery.user_id).first()
        if user:
            return user.username, gallery.filename
        else:
            return None, None
    else:
        return None, None

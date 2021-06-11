import os

POSTGRES_USER= os.getenv('CONNECTION_USER', 'postgres')
POSTGRES_PW=os.getenv('CONNECTION_PASSWORD', 'postgre5')
POSTGRES_URL="{server}:{port}".format(server=os.getenv('CONNECTION_SERVER', 'localhost'),port=os.getenv('CONNECTION_PORT', '5432'))
POSTGRES_DB_DEV=os.getenv('CONNECTION_DB', 'cifor_geodb')
POSTGRES_DB_TEST="inageo_dev_test"

DB_URL_DEV = 'postgresql://{user}:{pw}@{url}/{db}'.format(user=POSTGRES_USER,pw=POSTGRES_PW,url=POSTGRES_URL,db=POSTGRES_DB_DEV)
DB_URL_TEST = 'postgresql://{user}:{pw}@{url}/{db}'.format(user=POSTGRES_USER,pw=POSTGRES_PW,url=POSTGRES_URL,db=POSTGRES_DB_TEST)

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'my_pr3cious_secret_key')
    DEBUG = False

class DevelopmentConfig(Config):
    # uncomment the line below to use postgres
    SQLALCHEMY_DATABASE_URI = DB_URL_DEV
    DEBUG = True
    #SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_main.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = DB_URL_TEST #'sqlite:///' + os.path.join(basedir, 'flask_boilerplate_test.db')
    PRESERVE_CONTEXT_ON_EXCEPTION = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    DEBUG = False
    # uncomment the line below to use postgres
    SQLALCHEMY_DATABASE_URI = DB_URL_TEST


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY

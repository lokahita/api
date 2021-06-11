import os

from app.main import create_app
from app import blueprint

app = create_app(os.getenv('INAGEOPORTAL_ENV') or 'dev')

app.register_blueprint(blueprint)


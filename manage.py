import os
import unittest

from flask_script import Manager

from flask_migrate import Migrate, MigrateCommand

from app.main import create_app, db
from app.main.model import blacklist

from app import blueprint

from flask_cors import CORS

app = create_app(os.getenv('CIFOR_ENV') or 'dev')

manager = Manager(app)

migrate = Migrate(app, db)

manager.add_command('db', MigrateCommand)

app.register_blueprint(blueprint)
app.app_context().push()


# enable CORS
CORS(app, resources={r'/*': {'origins': '*'}})


@manager.command
def run():
    app.run(host='0.0.0.0', port=8004)


@manager.command
def test():
    """Runs the unit tests."""
    tests = unittest.TestLoader().discover('app/test', pattern='test*.py')
    result = unittest.TextTestRunner(verbosity=2).run(tests)
    if result.wasSuccessful():
        return 0
    return 1

if __name__ == '__main__':
    manager.run()
from .. import db
from marshmallow import fields, Schema
import datetime

class Statistic(db.Model):
    """ Statistic Model for storing statistic related details """
    __tablename__ = "m_statistic"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(255), nullable=False)
    created_date = db.Column(db.DateTime)
    file_name = db.Column(db.String(255))

    # class constructor
    def __init__(self, data):
        """
        Class constructor
        """
        self.id = data.get('id')
        self.username = data.get('username')
        self.created_date = datetime.datetime.utcnow()
        self.file_name = data.get('file_name')

    def save(self):
        db.session.add(self)
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    @staticmethod
    def get_all_statistic():
        return Statistic.query.all()

    @staticmethod
    def get_statistic_by_id(id):
        return Statistic.query.get(id)

    @staticmethod
    def get_statistic_by_username(username):
        return Statistic.query.filter_by(username=username).first()
  
    def __repr__(self):
        return '<id {}>'.format(self.id)

class StatisticSchema(Schema):
    """
    Statistic Schema
    """
    id = fields.Int(dump_only=True)
    username = fields.Str(required=True)
    created_date = fields.DateTime(required=True)
    file_name = fields.Str(required=True)
    
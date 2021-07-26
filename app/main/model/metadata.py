from .. import db
from geoalchemy2 import Geometry

class Metadata(db.Model):
    """ Metadata Model for storing metadata related details """
    __tablename__ = 'metadata'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    filename = db.Column(db.String(255), nullable=False)
    time_uploaded = db.Column(db.DateTime, nullable=False)
    validated = db.Column(db.Boolean, nullable=False, default=False)
    status = db.Column(db.Boolean, nullable=False, default=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    users = db.relationship('User', backref=db.backref('parent', lazy='dynamic'))
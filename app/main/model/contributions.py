from .. import db

class Contributions(db.Model):
    """ Contributions Model for storing contributions related details """
    __tablename__ = 'contributions'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    data_name = db.Column(db.String(100), nullable=False)
    filename = db.Column(db.String(100), nullable=False)
    time_uploaded = db.Column(db.DateTime, nullable=False)
    url = db.Column(db.String(255), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    users = db.relationship('User', backref=db.backref('parent_contribution', lazy='dynamic'))

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name != 'id'}
    

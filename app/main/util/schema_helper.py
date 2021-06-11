from flask import abort, jsonify

from app.main import db
from app.main.model.m_province import M_Province


def get_count_format(results):
    # check if page exists
    #results = klass.query.all()
    count = len(results)
    # make response
    obj = {}
    obj['count'] = count
    # make URLs
    # make previous url
    data = []
    for i in results: 
        #print(i.as_dict())
        data.append(i.as_dict()) 
    obj['data'] = data
    return jsonify(obj)
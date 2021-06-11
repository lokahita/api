from flask import abort, jsonify

from app.main import db
from app.main.model.user import User


def get_paginated_list(results, url, start, limit):
    # check if page exists
    #results = klass.query.all()
    count = len(results)
    if (count < start):
        abort(404)
    # make response
    obj = {}
    obj['start'] = start
    obj['limit'] = limit
    obj['count'] = count
    # make URLs
    # make previous url
    if start == 1:
        obj['previous'] = ''
    else:
        start_copy = max(1, start - limit)
        limit_copy = start - 1
        obj['previous'] = url + '?start=%d&limit=%d' % (start_copy, limit_copy)
    # make next url
    if start + limit > count:
        obj['next'] = ''
    else:
        start_copy = start + limit
        obj['next'] = url + '?start=%d&limit=%d' % (start_copy, limit)
    # finally extract result according to bounds
    #print(type(results[(start - 1):(start - 1 + limit)]))
    data = results[(start - 1):(start - 1 + limit)]
    results = []
    for i in data: 
        #print(i.as_dict())
        results.append(i.as_dict()) 
    obj['results'] = results
    return obj
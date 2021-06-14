import uuid
import datetime

from app.main import db
from app.main.model.organizations import Organizations
from app.main.model.harvestings import Harvestings
from sqlalchemy import func, extract, desc
from flask.json import jsonify
from owslib.csw import CatalogueServiceWeb
import json

# convert to string
#input = json.dumps({'id': id })

# load to dict
#my_dict = json.loads(input) 

def harvest_an_organization(id):
    row = Organizations.query.filter_by(id=id).first()
    if row:
        #print(row.__dict__)
        csw = CatalogueServiceWeb(row.csw, timeout=30)
        get_csw_records(csw, pagesize=10, maxrecords=3000)
        #data = []
        total = 0
        for rec in csw.records:
            #print(csw.records[rec].title , " ", csw.records[rec].type, " ", csw.records[rec].modified)
            if csw.records[rec].type == 'dataset' :
                if  csw.records[rec].modified == None:
                    mod = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')
                elif len(csw.records[rec].modified) == 10:
                    mod = datetime.datetime.strptime(csw.records[rec].modified, "%Y-%m-%d")
                elif len(csw.records[rec].modified) > 19:
                    mod = datetime.datetime.strptime(csw.records[rec].modified, "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    mod = datetime.datetime.strptime(csw.records[rec].modified, "%Y-%m-%dT%H:%M:%S")
                minx = csw.records[rec].bbox.minx
                miny = csw.records[rec].bbox.miny
                maxx = csw.records[rec].bbox.maxx
                maxy = csw.records[rec].bbox.maxy
                new_harvest = Harvestings(
                    organization_id=row.id,
                    title=csw.records[rec].title,
                    data_type=csw.records[rec].type,
                    abstract=csw.records[rec].abstract,
                    identifier=csw.records[rec].identifier,
                    modified=mod,
                    references=json.dumps(csw.records[rec].references),
                    subjects=json.dumps(csw.records[rec].subjects),
                    bbox='SRID=4326;POLYGON(('+minx+' '+miny+','+maxx+' '+miny+','+maxx+' '+maxy+','+minx+' '+maxy+','+minx+' '+miny+'))'
                )
                save_changes(new_harvest)
                total = total+1          
        response_object = {
                'status': 'ok',
                'name': row.name,
                'csw': row.csw,
                'total': total
                #'data': data
        }
        return response_object, 200
    else:
        response_object = {
                'status': 'fail',
                'message': 'not found',
            }
        return response_object, 200

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

def get_all():
    #return Harvestings.query.order_by('title').all()
    return Harvestings.query.order_by('title').all()
    ##limit(10).all()
    '''
    if page_size:
        query = query.limit(page_size)
    if page: 
        query = query.offset(page*page_size)
    '''

def get_harvest_by_organization_id(id):
    return Harvestings.query.filter_by(organization_id=id).order_by('title').all()

def get_harvest_by_identifier(id):
    return Harvestings.query.filter_by(identifier=id).order_by('title').first()

def get_bbox_by_identifier(id):
    query =  db.session.query(Harvestings.bbox.ST_AsGeoJSON()).filter(Harvestings.identifier == id).first()
    if (query):
        #print(query[1])
        response_object = {
                'status': 'ok',
                'message': {
                    'bbox' : query[0],
                }
        }
        return response_object, 200
    else:
        response_object = {
                'status': 'failed',
                'message': 'id not found',
            }

    return response_object, 200
    

#query = session.query(Lake).filter(Lake.geom.ST_Contains('POINT(4 1)'))
def get_harvest_by_bbox(minx, miny, maxx, maxy):
    return Harvestings.query.filter(Harvestings.bbox.ST_Intersects('SRID=4326;POLYGON(('+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+'))')).order_by('title').all()
    #.limit(100)
    #return Harvestings.query.filter(Harvestings.bbox.ST_Within('SRID=4326;POLYGON(('+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+'))')).order_by('title').limit(50).all()

def get_harvest_by_bbox_theme(minx, miny, maxx, maxy, theme):
    harvesting = Harvestings.Harvestings.query.filter(Harvestings.bbox.ST_Intersects('SRID=4326;POLYGON(('+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+'))')).all()
    search = "%{}%".format(theme)
    #print(search)
    return harvesting.query.filter(harvesting.name.ilike(search)).all()


def save_changes(data):
    db.session.add(data)
    db.session.commit()


def delete_harvested(data):
    harvesting = Harvestings.query.filter_by(id=data['id']).first()
    if harvesting:
        harvesting.query.filter_by(id=data['id']).delete()
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


def get_count_all():
    #return Harvestings.query.order_by('title').all()
    response_object = {
            'count': Harvestings.query.count(),
    }
    return response_object, 200

def get_count_organization():
    query =  db.session.query(Organizations.name, func.count(Harvestings.organization_id)).join(Organizations).order_by(Organizations.name).group_by(Organizations.name).all()
    print(query)
    data = []
    for i in query: 
        #print(i.as_dict())
        print(i)
        data.append({
                    'organization_name' : i[0],
                    'count' : i[1]
                }
        )
    print(data)
    response_object = {
            'data': data
    }
    return response_object, 200

def get_count_year():
    result = db.session.query(extract('year',Harvestings.modified).label('year'), func.count('year')).order_by(desc('year')).group_by('year').all()
    print(result)
    #results = [dict(row) for row in result]
    #results = [list(row) for row in result]
    results =  []
    for i in result: 
        #print(i.as_dict())
        print(i)
        results.append({
                    'year' : i[0],
                    'count' : i[1]
                }
        )
    print(results)
    result_dict = {'data': results}
    #response_object = {
    #        'status': 'oke',
    #        'data': jsonify([dict(row) for row in result])
    #}
    return result_dict, 200




def get_limit(order="title", page=0, page_size=None):
    #return Harvestings.query.order_by('title').all()
    ##limit(10).all()
    query = Harvestings.query.order_by(Harvestings.modified)
    if page_size:
        query = query.limit(page_size).all()
    if page: 
        query = query.offset(page*page_size).all()
    #print(query)
    return query

def get_latest():
    #return Harvestings.query.order_by('title').all()
    ##limit(10).all()
    query = Harvestings.query.order_by(desc(Harvestings.modified)).limit(24).all()
    return query

def get_alphabet():
    #return Harvestings.query.order_by('title').all()
    ##limit(10).all()
    query = Harvestings.query.order_by(Harvestings.title).limit(24).all()
    return query


    #for row in query:
    #    print(row)
    
    #results = [dict(row) for row in query]
    #print(results)
    #result_dict = {'data': []}
    #response_object = {
    #        'status': 'oke',
    #        'data': jsonify([dict(row) for row in result])
    #}
    #return result_dict, 200
    
    '''
    if page_size:
        query = query.limit(page_size)
    if page: 
        query = query.offset(page*page_size)
    '''

    '''
    query =  db.session.query(Harvestings.organization_id, func.count(Harvestings.organization_id)).group_by(Harvestings.organization_id).all()
    print(query)
    if (query):
        #print(query[1])
        response_object = {
                'status': 'ok',
                'message': {
                    'name' : query[0],
                    'bbox' : query[1]
                }
        }
    return response_object, 200
    '''
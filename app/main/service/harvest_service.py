import uuid
import datetime

from app.main import db
from app.main.model.organizations import Organizations
from app.main.model.harvestings import Harvestings
from sqlalchemy import func, extract, desc
from flask.json import jsonify
from owslib.csw import CatalogueServiceWeb
import json
from pyproj import Proj, transform
import os
import time
# convert to string
#input = json.dumps({'id': id })

# load to dict
#my_dict = json.loads(input) 
LOG_FOLDER = os.path.join(os.getcwd(), 'data/logs')

def harvest_an_organization(id):
    row = Organizations.query.filter_by(id=id).first()
    if row:
        #print(row.__dict__)
        #delete all
        harvesting = Harvestings.query.filter_by(organization_id=id).all()
        if harvesting:
            Harvestings.query.filter_by(organization_id=id).delete()
        csw = CatalogueServiceWeb(row.csw, timeout=30)
        get_csw_records(csw, pagesize=10, maxrecords=3000)
        #data = []
        total = 0
        #print(csw.records)
      
        strz = ''
        for rec in csw.records:
            #print(csw.records[rec].title , " ", csw.records[rec].type, " ", csw.records[rec].modified)
            if csw.records[rec].identifier:
                '''
                if csw.records[rec].type == 'dataset' :
                    
                    if csw.records[rec].modified == None:
                        mod = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')
                    elif len(csw.records[rec].modified) == 10:
                        mod = datetime.datetime.strptime(csw.records[rec].modified, "%Y-%m-%d")
                    elif len(csw.records[rec].modified) == 20:
                        csw.records[rec].datestampmod = datetime.datetime.strptime(csw.records[rec].modified, "%Y-%m-%dT%H:%M:%SZ")
                    elif len(csw.records[rec].modified) > 20:
                        mod = datetime.datetime.strptime(csw.records[rec].modified, "%Y-%m-%dT%H:%M:%S.%fZ")
                    else:
                        mod = datetime.datetime.strptime(csw.records[rec].modified, "%Y-%m-%dT%H:%M:%S")
                    minx = csw.records[rec].bbox.minx
                    miny = csw.records[rec].bbox.miny
                    maxx = csw.records[rec].bbox.maxx
                    maxy = csw.records[rec].bbox.maxy
                    #print(csw.records[rec].bbox.minx)
                    ''
                    inProj = Proj('epsg:4326')
                    outProj = Proj('epsg:3857')
                    x1,y1 = minx,miny
                    x2,y2 = transform(inProj,outProj,x1,y1)
                    x3,y3 = maxx,maxy
                    x4,y4 = transform(inProj,outProj,x3,y3)
                    print(x1,y1)
                    print(x2,y2)
                    print(x3,y3)
                    print(x4,y4)
                '''
                minx = csw.records[rec].identification.bbox.minx
                miny = csw.records[rec].identification.bbox.miny
                maxx = csw.records[rec].identification.bbox.maxx
                maxy = csw.records[rec].identification.bbox.maxy
                tipe = ""
                if csw.records[rec].identification.spatialrepresentationtype:
                    tipe=csw.records[rec].identification.spatialrepresentationtype[0]
                
                try:     
                    #print(i, " ", csw_target.records[rec].identifier, " ", jum )
                    distributions = csw.records[rec].distribution.online
                    check = False
                    for dist in distributions:
                        #print("         ", dist.name, " ", dist.protocol)
                        if 'WMS' in dist.url:
                            check = True
                    if check:
                        #print(i, " ", csw_target.records[rec].identifier, " 0" )
                        new_harvest = Harvestings(
                            organization_id=row.id,
                            title=csw.records[rec].identification.title,
                            data_type=tipe,
                            abstract=csw.records[rec].identification.abstract,
                            identifier=csw.records[rec].identifier,
                            publication_date=csw.records[rec].datestamp,#datetime.datetime.strptime(, "%Y-%m-%dT%H:%M:%SZ"),
                            distributions=json.dumps([i.__dict__ for i in csw.records[rec].distribution.online]),
                            categories=json.dumps([i for i in csw.records[rec].identification.topiccategory]),
                            keywords=json.dumps([i for i in csw.records[rec].identification.keywords]),
                            #bbox='SRID=3857;POLYGON(('+str(x2)+' '+str(y2)+','+str(x4)+' '+str(y2)+','+str(x4)+' '+str(y4)+','+str(x2)+' '+str(y4)+','+str(x2)+' '+str(y2)+'))'
                            bbox='SRID=4326;POLYGON(('+minx+' '+miny+','+maxx+' '+miny+','+maxx+' '+maxy+','+minx+' '+maxy+','+minx+' '+miny+'))'
                        )
                        strz = strz + '\n' + csw.records[rec].identifier + ',' + csw.records[rec].identification.title
                        save_changes(new_harvest) 
                        total = total+1
                except AttributeError:
                    print(csw.records[rec].identifier, " 0")
                    #i=i+1
                    continue
                    #print(total)
        filename =  time.strftime(row.name +'_%Y%m%d_%H%M%S_'+str(total)+'.txt', time.localtime())
        with open(os.path.join(LOG_FOLDER, filename), 'w') as f:
            f.write('Organization:' + row.name)
            f.write('\nCSW:' + row.csw)
            f.write('\nTotal:' + str(total))
            f.write(strz)
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
            outputschema="http://www.isotc211.org/2005/gmd",
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
    #query =  db.session.query(Harvestings.bbox.ST_AsGeoJSON()).filter(Harvestings.identifier == id).first()
    query =  db.session.query(Harvestings.bbox.ST_Transform(3857).ST_ASGeoJSON()).filter(Harvestings.identifier == id).first()
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
    if minx == -133044556.246885:
        return Harvestings.query.order_by(Harvestings.title).all()
    else:
        print(minx, miny, maxx, maxy)
        inProj = Proj('epsg:3857')
        outProj = Proj('epsg:4326')
        miny, minx = transform(inProj,outProj,minx,miny)
        maxy, maxx = transform(inProj,outProj,maxx,maxy)
        return Harvestings.query.filter(Harvestings.bbox.ST_ASText() != 'POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))').filter(Harvestings.bbox.ST_Intersects('SRID=4326;POLYGON(('+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+'))')).order_by('title').all()
    #else:
        #print(minx, miny, maxx, maxy)
        #harvesting = Harvestings.query.filter(Harvestings.bbox.ST_Intersects('SRID=4326;POLYGON(('+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+'))')).all()
        #search = "%{}%".format('forest')
        #print(search)
        #return harvesting.query.filter(harvesting.name.ilike(search)).all()

        #non_empty =  Harvestings.query.filter(Harvestings.bbox.ST_ASText() != 'POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))').all()
        #if non_empty:
            #print(type(non_empty))
        #    return non_empty.filter(non_empty.bbox.ST_Transform(3857).ST_Intersects('SRID=3857;POLYGON(('+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+'))')).order_by('title').all()
        #else:
        #    return None
        #return Harvestings.query.filter(Harvestings.bbox.ST_Intersects('SRID=3857;POLYGON(('+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+'))')).order_by('title').all()
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
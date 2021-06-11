from ldap3 import Connection, MODIFY_REPLACE

from app.main import server_ldap
from ..config import LDAP_MANAGER, LDAP_PASSWORD
from app.main.model.admin import Admin
from app.main.model.blacklist import BlacklistToken

from ..service.blacklist_service import save_token

def save_new_token(data):
    #print(data['username'], data['password'])
    with Connection(server_ldap, 'cn='+ data['username'] +',ou=People,dc=oam_user,dc=com', data['password']) as conn:
        #conn.start_tls()
        if(conn.bind()):
            print('valid')
            if(conn.search ('ou=People,dc=oam_user,dc=com', '(&(uid='+data['username'] +'))', attributes=['*'])):
                print('found')
                print(type(conn.entries))
                print(len(conn.entries))
                #print(conn.entries[0])
                print(conn.entries[0].uid)
                auth_token = Akun.encode_auth_token(data['username'])
                print(auth_token)
                print(type(auth_token))

                if auth_token:
                    response_object = {
                        'status': 'success',
                        'message': 'Successfully logged in',
                        'Authorization': auth_token.decode()
                    }
                    return response_object, 200
            else:
                response_object = {
                    'status': 'fail',
                    'message': 'not found'
                }
                return response_object, 200
                '''
                businessCategory: Government
                cn: emhayusa
                departmentNumber: PPIG
                destinationIndicator: INDONESIA
                givenName: muhammad
                l: Kabupaten Bogor
                mail: emhayusa@gmail.com
                o: 998
                objectClass: top
                            person
                            inetorgperson
                            organizationalperson
                            groupOfUniqueNames
                postalAddress: Jalan Raya Jakarta Bogor KM 46 Bogor
                postalCode: 16911
                registeredAddress: Jawa Barat
                sn: yusa
                telephoneNumber: +6285213773700
                title: Web GIS Programmer
                uid: emhayusa 
                '''
    response_object = {
        'status': 'fail',
        'message': 'username or password is not valid'
    }
    return response_object, 200
        #response_object = {
        #    'status': 'success',
        #    'message': 'Successfully registered.'
        #}
        #return response_object, 201

def validitas_akun(data):
    if data:
        auth_token = data #data.split(" ")[1]
    else:
        auth_token = ''
    if auth_token:
        resp = Akun.decode_auth_token(auth_token)
        if not isinstance(resp, str):
            print(resp)
            response_object = {
                'status': 'success',
                'message': resp
            }
            return response_object, 200
        else:
            response_object = {
                'status': 'fail',
                'message': resp
            }
            return response_object, 200
    else:
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        return response_object, 200

def logout_akun(data):
    if data:
        auth_token = data #data.split(" ")[1]
    else:
        auth_token = ''
    if auth_token:
        resp = Akun.decode_auth_token(auth_token)
        if not isinstance(resp, str):
            # mark the token as blacklisted
            return save_token(token=auth_token)
        else:
            response_object = {
                'status': 'fail',
                'message': resp
            }
            return response_object, 200
    else:
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        return response_object, 200
'''
def get_logged_in_developer(new_request):
        # get the auth token
        auth_token = new_request.headers.get('Authorization')
        if auth_token:
            resp = Developer.decode_auth_token(auth_token)
            if not isinstance(resp, str):
                developer = Developer.query.filter_by(id=resp).first()
                response_object = {
                    'status': 'success',
                    'data': {
                        'developer_id': developer.id,
                        'email': developer.email,
                        'admin': developer.admin,
                        'registered_on': str(developer.registered_on)
                    }
                }
                return response_object, 200
            response_object = {
                'status': 'fail',
                'message': resp
            }
            return response_object, 401
        else:
            response_object = {
                'status': 'fail',
                'message': 'Provide a valid auth token.'
            }
            return response_object, 401
'''
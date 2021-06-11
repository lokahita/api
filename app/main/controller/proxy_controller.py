import re
from urllib.parse import urlparse, urlunparse
from flask import Flask, render_template, request, abort, Response, redirect
import requests
import logging
from ..util.dto import ProxyDto

from flask_restplus import Resource

logging.basicConfig(level=logging.INFO)
APPROVED_HOSTS = set(["google.com", "www.google.com", "yahoo.com"])
CHUNK_SIZE = 1024
LOG = logging.getLogger("app.py")


api = ProxyDto.api

@api.route('/<path:url>')
@api.param('url', 'The Url')
class proxyDtoList(Resource):
    @api.doc('list of organizations')
    #@api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self, url):
        """List all organizations"""
        referer = request.headers.get('referer')
        print(referer)
        if not referer:
            return Response("Relative URL sent without a a proxying request referal. Please specify a valid proxy host (/p/url)", 400)
        proxy_ref = proxied_request_info(referer)
        print(proxy_ref)
        host = ''
        if proxy_ref:
            host = proxy_ref[0]
        redirect_url = "/p/%s/%s%s" % (host, url, ("?" + request.query_string.decode('utf-8') if request.query_string else ""))
        return 'Hello ' + referer 

@api.route('/p/<path:url>')
@api.param('url', 'The Url')
class proxyDtoList2(Resource):
    @api.doc('list of organizations')
    #@api.marshal_list_with(_schema, envelope='data')
    #@admin_token_required
    def get(self, url):
        """List all organizations"""
        print(url)
        url_parts = urlparse('%s://%s' % (request.scheme, url))
        print(url_parts)
        if url_parts.path == "":
            parts = urlparse(request.url)
            print("Proxy request without a path was sent, redirecting assuming '/': %s -> %s/" % (url, url))
            return 'hello'
            #return redirect(urlunparse(parts._replace(path=parts.path+'/')))
        #url = 'http://landscapeportal.org/geoserver/wms?service=WMS&request=GetMap&layers=geonode%3Akenya_nyando_basin_landtenure1964&styles=&format=image%2Fpng&transparent=true&version=1.1.1&width=256&height=256&srs=EPSG%3A3857&bbox=4070118.882129066,-156543.033928041,4148390.3990930864,-78271.51696401955'
        print("%s %s with headers: %s", request.method, url, request.headers)
        r = make_request(url, request.method, dict(request.headers), request.form)
        #print(r)
        #out = r
        print("Got %s response from %s",r.status_code, url)
        headers = dict(r.raw.headers)
        def generate():
            for chunk in r.raw.stream(decode_content=False):
                yield chunk
        out = Response(generate(), headers=headers)
        out.status_code = r.status_code
        return out

       

'''
@api.route('/<path:url>', methods=["GET", "POST"])
def root(url):
    # If referred from a proxy request, then redirect to a URL with the proxy prefix.
    # This allows server-relative and protocol-relative URLs to work.
    referer = request.headers.get('referer')
    if not referer:
        return Response("Relative URL sent without a a proxying request referal. Please specify a valid proxy host (/p/url)", 400)
    proxy_ref = proxied_request_info(referer)
    host = proxy_ref[0]
    redirect_url = "/p/%s/%s%s" % (host, url, ("?" + request.query_string.decode('utf-8') if request.query_string else ""))
    LOG.debug("Redirecting relative path to one under proxy: %s", redirect_url)
    return redirect(redirect_url)


@api.route('/p/<path:url>', methods=["GET", "POST"])
def proxy(url):
    """Fetches the specified URL and streams it out to the client.
    If the request was referred by the proxy itself (e.g. this is an image fetch
    for a previously proxied HTML page), then the original Referer is passed."""
    # Check if url to proxy has host only, and redirect with trailing slash
    # (path component) to avoid breakage for downstream apps attempting base
    # path detection
    url_parts = urlparse('%s://%s' % (request.scheme, url))
    if url_parts.path == "":
        parts = urlparse(request.url)
        LOG.warning("Proxy request without a path was sent, redirecting assuming '/': %s -> %s/" % (url, url))
        return redirect(urlunparse(parts._replace(path=parts.path+'/')))

    LOG.debug("%s %s with headers: %s", request.method, url, request.headers)
    r = make_request(url, request.method, dict(request.headers), request.form)
    LOG.debug("Got %s response from %s",r.status_code, url)
    headers = dict(r.raw.headers)
    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk
    out = Response(generate(), headers=headers)
    out.status_code = r.status_code
    return out
'''

def make_request(url, method, headers={}, data=None):
    url = 'http://%s' % url
    # Ensure the URL is approved, else abort
    print(url)
    #if not is_approved(url):
    #    print("URL is not approved: %s", url)
    #    abort(403)

    # Pass original Referer for subsequent resource requests
    referer = request.headers.get('referer')
    if referer:
        proxy_ref = proxied_request_info(referer)
        print(proxy_ref)
        #headers.update({ "referer" : "http://%s/%s" % (proxy_ref[0], proxy_ref[1])})
    
    print(request.args)

    # Fetch the URL, and stream it back
    #print("Sending %s %s with headers: %s and data %s", method, url, headers, data)
    print("Sending %s %s ", method, url)
    #return "Sending %s %s ", method, url
    #return requests.request('GET', url)
    return requests.request(method, url, params=request.args, stream=True, headers=headers, allow_redirects=False, data=data)

def is_approved(url):
    """Indicates whether the given URL is allowed to be fetched.  This
    prevents the server from becoming an open proxy"""
    parts = urlparse(url)
    print(parts)
    print(APPROVED_HOSTS)
    return parts.netloc in APPROVED_HOSTS

def proxied_request_info(proxy_url):
    """Returns information about the target (proxied) URL given a URL sent to
    the proxy itself. For example, if given:
        http://localhost:5000/p/google.com/search?q=foo
    then the result is:
        ("google.com", "search?q=foo")"""
    parts = urlparse(proxy_url)
    if not parts.path:
        return None
    elif not parts.path.startswith('/p/'):
        return None
    matches = re.match('^/p/([^/]+)/?(.*)', parts.path)
    proxied_host = matches.group(1)
    proxied_path = matches.group(2) or '/'
    proxied_tail = urlunparse(parts._replace(scheme="", netloc="", path=proxied_path))
    LOG.debug("Referred by proxy host, uri: %s, %s", proxied_host, proxied_tail)
    return [proxied_host, proxied_tail]
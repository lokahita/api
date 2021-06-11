########
# This image will compile the dependencies
# It will install compilers and other packages, that won't be carried
# over to the runtime image
########
FROM python:3.7.5-slim-buster AS compile-image

# Add requirements for python and pip
#RUN apk add --update python3
RUN apt-get update 
RUN apt-get -y install python3

RUN mkdir -p /opt/code
WORKDIR /opt/code
# Install dependencies
#RUN apk add python3-dev build-base gcc linux-headers postgresql-dev libffi-dev
RUN apt-get -y install python3-dev
RUN apt-get -y install gcc
RUN apt-get -y install libpq-dev
RUN apt-get -y install libffi-dev
#RUN apt-get -y install libldap2-dev 
#RUN apt-get -y install libsasl2-dev 
#RUN apt-get -y install libssl-dev

# Create a virtual environment for all the Python dependencies
RUN python3 -m venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"
RUN pip3 install --upgrade pip
# Install and compile uwsgi
RUN pip3 install uwsgi==2.0.18
#RUN pip3 install flask_script
#RUN pip3 install flask_migrate
#RUN pip3 install flask_restplus
#RUN pip3 install Werkzeug==0.16.1
#RUN pip3 install flask_bcrypt
#RUN pip3 install geoalchemy2
#RUN pip3 install marshmallow
#RUN pip3 install flask_cors
#RUN pip3 install owslib
#RUN pip3 install psycopg2

# Install other dependencies
COPY requirements.txt /opt/
RUN pip3 install -r /opt/requirements.txt

########
# This image is the runtime, will copy the dependencies from the other
########
FROM python:3.7.5-slim-buster AS runtime-image

# Install python
#RUN apk add --update python3 curl libffi postgresql-libs
RUN apt-get update
RUN apt-get -y install python3
RUN apt-get -y install curl
RUN apt-get -y install libffi-dev
RUN apt-get -y install libpq-dev

# Copy uWSGI configuration
RUN mkdir -p /opt/uwsgi
ADD uwsgi.ini /opt/uwsgi/
ADD start_server.sh /opt/uwsgi/
# Create a user to run the service
#RUN addgroup uwsgi
#RUN useradd -ms /bin/sh uwsgi
RUN adduser uwsgi
USER uwsgi
# Copy the venv with compile dependencies from the compile-image
COPY --chown=uwsgi:uwsgi --from=compile-image /opt/venv /opt/venv
# Be sure to activate the venv
ENV PATH="/opt/venv/bin:$PATH"
# Copy the code
# COPY --chown=uwsgi:uwsgi app/ /opt/code/app/
COPY --chown=uwsgi:uwsgi wsgi.py /opt/code/
# Run parameters
WORKDIR /opt/code
EXPOSE 5000
CMD ["/bin/sh", "/opt/uwsgi/start_server.sh"]

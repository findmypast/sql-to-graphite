FROM ubuntu:xenial

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
        curl \
        lsb-release \
        locales \
        libwxgtk3.0-dev \
        libsctp1 \
        libglu-dev \
        apt-transport-https \
        git \
        netcat \
        python-pip \
        libmysqlclient-dev \
        freetds-bin

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list | tee -a /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install msodbcsql -y \
  && apt-get install unixodbc-dev -y

# Set the locale
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Install sql-to-graphite
RUN pip install redshift-sqlalchemy \
  && pip install pyodbc

COPY . sql-to-graphite-master/
RUN pip install -e sql-to-graphite-master/

COPY odbc.ini /etc/odbc.ini
COPY show_odbc_sources.py .

CMD cat $SQL_FILE | sql-to-graphite --graphite-host $GRAPHITE_HOST --graphite-prefix $PREFIX --timestamped-metric

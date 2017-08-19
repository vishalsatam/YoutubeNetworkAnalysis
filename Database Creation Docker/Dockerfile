FROM ubuntu:latest

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    python-pip --upgrade python-pip

RUN pip install --upgrade pip

# install py3
RUN apt-get update -qq \
 && apt-get install --no-install-recommends -y \
    # install python 3
    python3 \
    python3-dev \
    python3-pip \
    libmysqlclient-dev \
    python3-setuptools \
    jq \
    pkg-config \
	vim &&\
	echo "luigi ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    apt-get clean && \
	rm -rf /var/lib/apt/lists/*

#RUN DEBIAN_FRONTEND=noninteractive apt-get -y install mysql-client
#RUN echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list


RUN apt-get update

RUN apt-get -y install mysql-client

# install additional python packages
RUN pip3 install ipython
RUN pip install jupyter
RUN pip3 install numpy
RUN pip3 install pandas
RUN pip3 install scikit-learn
RUN pip3 install matplotlib
RUN pip3 install missingno
RUN pip3 install scipy
RUN pip3 install seaborn
#RUN pip install nltk
RUN pip3 install boto3
RUN pip3 install requests
RUN pip3 install plotly
RUN pip3 install ConfigParser
RUN pip3 install --upgrade setuptools
RUN pip install --upgrade setuptools
RUN pip install mysql-connector==2.1.4
RUN pip3 install folium
RUN pip3 install --upgrade awscli

RUN pip3 install luigi

RUN pip3 install jupyter
RUN pip install --upgrade awscli

# configure console
RUN echo 'alias ll="ls --color=auto -lA"' >> /root/.bashrc \
 && echo '"\e[5~": history-search-backward' >> /root/.inputrc \
 && echo '"\e[6~": history-search-forward' >> /root/.inputrc
# default password: keras
ENV PASSWD='sha1:98b767162d34:8da1bc3c75a0f29145769edc977375a373407824'

# dump package lists
RUN dpkg-query -l > /dpkg-query-l.txt \
 && pip2 freeze > /pip2-freeze.txt \
 && pip3 freeze > /pip3-freeze.txt

# for jupyter
EXPOSE 8888

#expose for mysql
EXPOSE 3306

WORKDIR /src/

RUN mkdir /src/finalproj
RUN mkdir /src/finalproj/sql
RUN mkdir /src/finalproj/data
RUN mkdir /src/finalproj/logs
RUN mkdir /src/finalproj/config
#RUN mkdir /src/finalproj/JupyterNotebooksDBAndApi
ENV MAINPATH=/src/finalproj
ENV SCRIPTSPATH=/src/finalproj/sql
ENV LOGPATH=/src/finalproj/logs
ENV CONFIGPATH=/src/finalproj/config
ENV DATAPATH=/src/finalproj/data
ENV PYTHONPATH=$MAINPATH:$PYTHONPATH

ADD scripts/SetupNeo4JAndSparkEC2Environment.sh /src/finalproj/
ADD scripts/getFromS3AndLoadRDS.sh /src/finalproj/
ADD scripts/ExecuteInNeo4jEC2Instance.sh /src/finalproj/
ADD config/config.txt /src/finalproj/config/
ADD sql/* /src/finalproj/sql/
#ADD data/* /src/finalproj/data/
#ADD JupyterNotebooksDBAndApi/* /src/assignment2/JupyterNotebooksDBAndApi/


RUN chmod 777 $MAINPATH/getFromS3AndLoadRDS.sh
RUN chmod 777 $MAINPATH/SetupNeo4JAndSparkEC2Environment.sh
RUN chmod 777 $MAINPATH/ExecuteInNeo4jEC2Instance.sh
RUN chmod 777 $CONFIGPATH/config.txt
RUN chmod 777 $SCRIPTSPATH/*
RUN chmod 777 $DATAPATH
#RUN chmod 777 /src/finalproj/JupyterNotebooksDBAndApi/*

CMD $MAINPATH/getFromS3AndLoadRDS.sh

#RUN mkdir /srv/nb1/

#CMD /bin/bash -c 'jupyter notebook --no-browser --allow-root --ip=* --NotebookApp.password="$PASSWD" "$@"'

#CMD /bin/bash -c 'jupyter notebook --no-browser --ip=* --NotebookApp.password="keras" "$@"'

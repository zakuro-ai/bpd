FROM jupyter/pyspark-notebook

USER root
RUN sudo userdel jovyan
RUN sudo rm -r /home/jovyan

## Add foo user
RUN apt update -y && apt install openssh-server nano sudo rsync git nano wget -y
RUN adduser --disabled-password --gecos '' foo
RUN adduser foo sudo
RUN sudo usermod -u 1000 foo
RUN sudo usermod -g 100 foo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN echo 'export PYTHONPATH=/workspace' >> /home/foo/.bashrc
ENV HOME /home/foo
ENV XDG_CACHE_HOME=/home/foo/.cache/
ENV NB_USER foo

RUN rm /usr/local/bin/start.sh
COPY docker/vanilla/static/start.sh /usr/local/bin/
WORKDIR /workspace
RUN sudo chown foo:foo /workspace

USER foo
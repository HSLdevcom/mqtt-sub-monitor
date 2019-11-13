FROM continuumio/miniconda

ADD src /app/src
WORKDIR /app/src
RUN conda env create -f env_mqtt.yml
ENV PATH /opt/conda/envs/mqtt-env/bin:$PATH

CMD python main.py

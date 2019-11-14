FROM continuumio/miniconda3

ADD src /app/src
WORKDIR /app/src
RUN conda env create -f env_mqtt.yml && conda clean -afy
ENV PATH /opt/conda/envs/mqtt-env/bin:$PATH

CMD python main.py

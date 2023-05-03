FROM continuumio/miniconda3:4.9.2
WORKDIR /app/

# Install pangolin
COPY pangolin/environment.yml /pangolin/environment.yml
RUN conda env create -f /pangolin/environment.yml
COPY pangolin/ /pangolin/
RUN ls -l
RUN ls -l /pangolin

RUN /opt/conda/bin/conda run -n pangolin python -m pip install /pangolin
RUN /opt/conda/bin/conda run -n pangolin pangolin --update

# Prepare base python
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY import_pangolin_lineages.py .

ENTRYPOINT ["python3", "import_pangolin_lineages.py"]

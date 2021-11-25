FROM continuumio/miniconda3:latest
WORKDIR /app/

# Install pangolin
COPY pangolin/environment.yml /pangolin/environment.yml
RUN conda env create -f /pangolin/environment.yml
COPY pangolin/ /pangolin/
SHELL ["conda", "run", "-n", "pangolin", "/bin/bash", "-c"]
RUN cd /pangolin && python setup.py install
RUN ["conda", "run", "--no-capture-output", "-n", "pangolin", "pangolin", "--update"]


# Prepare base python
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY import_pangolin_lineages.py .

ENTRYPOINT ["python3", "import_pangolin_lineages.py"]

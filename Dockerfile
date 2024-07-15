FROM continuumio/miniconda3

WORKDIR /src/cads-e2e-tests

COPY environment.yml /src/cads-e2e-tests/

RUN conda install -c conda-forge gcc python=3.11 \
    && conda env update -n base -f environment.yml

COPY . /src/cads-e2e-tests

RUN pip install --no-deps -e .

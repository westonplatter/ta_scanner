FROM continuumio/miniconda3

# make docker use bash instead of sh
SHELL ["/bin/bash", "--login", "-c"]

# create conda myenv
COPY environment.yml .
RUN conda env create -f environment.yml

# copy files over
COPY restart.py .
COPY entrypoint.sh /usr/local/bin/

# make entrypoint script executable
RUN chmod u+x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

CMD ["python", "restart.py"]
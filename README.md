# SDTD Project - Application Repository

## This is a bare clone from https://gitlab.com/viviane.qian/projet-sdtd
This repository contains the source code of application for https://github.com/joshnah/velocite-deployment

- airflow/dags/: Application DAGs. Airflow's git-sync is configured to automatically retrieve DAGs from this repository.
- kafka/producer/: Python program for data retrieval
- spark/: Spark programs


## CI/CD
With every push to the repository, the Gitlab CI/CD pipeline is triggered. It builds the **producer** and **spark** images and pushes them to the Gitlab Registry.

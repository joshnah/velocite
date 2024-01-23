# Projet SDTD - Dépôt applicatif
Ce dépôt contiene le code source de l'application de démonstration du projet SDTD.

Lien du dépôt de déploiment: https://gitlab.com/phanti/projet-sdtd-k8s
- airflow/dags/ : les DAGs de l'application. Le git-sync de airflow est configuré pour récupérer les DAGs de ce dépôt automatiquement.
- kafka/producer/: Python programme pour la récupération des données
- spark/: les programmes Spark


## CI/CD
À chaque push sur le dépôt, le pipeline Gitlab CI/CD est déclenché. Il construit l'image **producer** et **spark** et les pousse sur le dépôt Gitlab Registry.
"""
DAG : auto_insurance_uc_databricks_pipeline
-------------------------------------------

Pipeline d’ingestion et de transformation de données d’assurance auto,
orchestré avec Airflow et exécuté sur Databricks Serverless.

Fonctionnalités :
- Lecture dynamique du Job ID Databricks via Variable Airflow
- Lecture dynamique du schedule CRON et du fuseau horaire
- FileSensor pour attendre l’arrivée du fichier source
- Déclenchement d’un Job Databricks existant via RunNowOperator
- Validation robuste des variables (erreurs explicites)
- Code réutilisable et maintenable
"""

import os
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sensors.filesystem import FileSensor

from airflow import DAG

# ---------------------------------------------------------------------------
# 1. Paramètres généraux du DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "souleymane_sow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# 2. Fonctions utilitaires (lecture et validation des Variables Airflow)
# ---------------------------------------------------------------------------


def _get_databricks_job_id() -> int:
    """
    Récupère le Job ID Databricks depuis :
    - une Variable Airflow (prioritaire)
    - une variable d’environnement (fallback)

    Valide que la valeur est bien un entier.
    """
    raw_job_id = Variable.get(
        "AUTO_INSURANCE_DATABRICKS_JOB_ID",
        default_var=os.getenv("AUTO_INSURANCE_DATABRICKS_JOB_ID"),
    )

    if raw_job_id is None:
        raise AirflowException(
            "❌ Databricks Job ID manquant.\n"
            "Définissez la variable Airflow : AUTO_INSURANCE_DATABRICKS_JOB_ID\n"
            "Exemple : airflow variables set AUTO_INSURANCE_DATABRICKS_JOB_ID 123456789"
        )

    try:
        return int(raw_job_id)
    except ValueError as exc:
        raise AirflowException(
            f"❌ AUTO_INSURANCE_DATABRICKS_JOB_ID doit être un entier. Reçu : {raw_job_id}"
        ) from exc


def _get_dag_schedule() -> str:
    """
    Récupère le CRON du DAG depuis une Variable Airflow.
    Par défaut : @daily.
    """
    return Variable.get("AUTO_INSURANCE_DAG_SCHEDULE", default_var="@daily")


def _get_dag_timezone() -> str:
    """
    Récupère le fuseau horaire du DAG.
    Par défaut : Europe/Paris.
    """
    return Variable.get("AUTO_INSURANCE_DAG_TIMEZONE", default_var="Europe/Paris")


# ---------------------------------------------------------------------------
# 3. Définition du DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="auto_insurance_uc_databricks_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz=_get_dag_timezone()),
    schedule_interval=_get_dag_schedule(),
    catchup=False,
    description="Pipeline assurance auto orchestré avec Airflow + Databricks (Serverless, UC, Bronze/Silver/Gold)",
) as dag:

    # -----------------------------------------------------------------------
    # 4. Tâche 1 : Attendre l’arrivée du fichier CSV
    # -----------------------------------------------------------------------
    wait_for_file = FileSensor(
        task_id="wait_for_auto_csv",
        filepath="/opt/airflow/data/Base_de_donnees.csv",
        poke_interval=30,  # vérifie toutes les 30 secondes
        timeout=600,  # timeout après 10 minutes
        mode="poke",
    )

    # -----------------------------------------------------------------------
    # 5. Tâche 2 : Déclencher le Job Databricks (Bronze → Silver → Gold)
    # -----------------------------------------------------------------------
    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="trigger_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=_get_databricks_job_id(),
    )

    # -----------------------------------------------------------------------
    # 6. Orchestration
    # -----------------------------------------------------------------------
    wait_for_file >> trigger_databricks_job

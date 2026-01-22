import pandas as pd
import sqlite3
import json
import openai
from datetime import datetime
from typing import Dict, List
from airflow import DAG
from airflow.operators.python import PythonOperator

class DataPipelineAI:
    def __init__(self, api_key: str):
        openai.api_key = api_key
        self.db_path = "datapipeline.db"
        self.init_database()

    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""CREATE TABLE IF NOT EXISTS pipelines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            description TEXT,
            config JSON,
            created_at TIMESTAMP,
            last_run TIMESTAMP,
            status TEXT,
            error_log JSON
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            status TEXT,
            records_processed INTEGER,
            errors JSON
        )""")

        conn.commit()
        conn.close()

    def analyze_data_source(self, source_path: str, source_type: str) -> Dict:
        if source_type == "csv":
            df = pd.read_csv(source_path, nrows=100)
        elif source_type == "excel":
            df = pd.read_excel(source_path, nrows=100)
        elif source_type == "json":
            df = pd.read_json(source_path)
        else:
            raise ValueError("Unsupported source type")

        return {
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "row_count": len(df)
        }

    def generate_pipeline(self, description: str, sources: List[Dict]) -> Dict:
        prompt = {
            "description": description,
            "sources": sources
        }

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": json.dumps(prompt)}],
            temperature=0.3
        )

        return json.loads(response.choices[0].message.content)

    def execute_pipeline(self, pipeline_config: Dict) -> Dict:
        start = datetime.now()
        try:
            print("Pipeline started:", pipeline_config.get("name"))
            end = datetime.now()
            return {
                "status": "success",
                "duration": (end - start).total_seconds()
            }
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }

    def generate_airflow_dag(self, pipeline_config: Dict) -> str:
        dag_id = pipeline_config.get("name", "pipeline").lower().replace(" ", "_")
        return f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_pipeline():
    print("Running pipeline")

with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline
    )
"""

if __name__ == "__main__":
    pipeline_ai = DataPipelineAI(api_key="your-openai-api-key")
    print("DataPipelineAI initialized")

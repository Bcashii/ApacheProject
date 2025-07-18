from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from airflow.exceptions import AirflowException
import requests

ID = '172'  
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_anime_api' 
ONE_DAY_AGO = datetime.now() - timedelta(days=7)

default_args = {
    'owner': 'airflow',
    'start_date': ONE_DAY_AGO,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='anime_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['anime', 'etl'],
) as dag:

    @task()
    def extract_anime_data():
        try:
            http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
            response = http_hook.run('encyclopedia/reports.xml?id=172')
            response.raise_for_status()
        
            soup = BeautifulSoup(response.text, 'lxml-xml')
        
            items = []
            for item in soup.find_all('item'):
                try:
                    anime_title_tag = item.find('anime')
                    if not anime_title_tag:
                        continue
                    
                    items.append({
                        'id': item.get('id'),  
                        'title': anime_title_tag.text.strip(),
                        'score': float(item.find('bayesian_average').text),
                        'nb_votes': int(item.find('nb_votes').text)  
                    })
                except (ValueError, AttributeError, TypeError) as e:
                    print(f"Skipping malformed item: {e}")
                    continue
        
            if not items:
                raise AirflowException("No valid items found in API response")
            
            print(f"Successfully extracted {len(items)} anime records")
            return {'items': items}
        
        except Exception as e:
            raise AirflowException(f"Failed to extract anime data: {str(e)}")

    @task()
    def transform_anime_data(anime_data):
        try:
            transformed_data = []
            skipped_count = 0
            
            
            for rank, item in enumerate(anime_data.get('items', []), start=1):
                try:
                    
                    if not item.get('id'):
                        skipped_count += 1
                        continue
                    
                    
                    item_id = int(item['id'])
                    if item_id <= 0:
                        print(f"Invalid ID {item_id} - must be positive")
                        skipped_count += 1
                        continue
                    
                    transformed_data.append({
                        'id': item_id,
                        'name': str(item.get('title', 'Unknown')).strip(),
                        'avg_score': max(0.0, min(10.0, float(item.get('score', 0.0)))),
                        'nb_votes': max(0, int(item.get('nb_votes', 0))),
                        'rank': rank  
                    })
                except (ValueError, TypeError) as e:
                    print(f"Skipping malformed item: {str(e)}\nItem: {item}")
                    skipped_count += 1
                    continue
            
            print(f"Transformed {len(transformed_data)} items (skipped {skipped_count})")
            if not transformed_data:
                raise AirflowException("No valid items found after transformation")
            
            return transformed_data
        except Exception as e:
            raise AirflowException(f"Failed to transform anime data: {str(e)}")

    @task()
    def load_anime_data(transformed_data):
        conn = None
        cursor = None
        try:
            postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
        
            
            cursor.execute("DROP TABLE IF EXISTS top_anime;")
            conn.commit()
        
            
            cursor.execute("""
                CREATE TABLE top_anime (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    avg_score FLOAT,
                    nb_votes INTEGER,
                    rank INTEGER
                );
            """)
            conn.commit()
        
            
            if not transformed_data:
                raise AirflowException("No data to load")
        
            records = [
                (r['id'], r['name'], r['avg_score'], r['nb_votes'], r['rank'])
                for r in transformed_data
            ]
        
            
            for record in records:
                try:
                    cursor.execute("""
                        INSERT INTO top_anime (id, name, avg_score, nb_votes, rank)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            avg_score = EXCLUDED.avg_score,
                            nb_votes = EXCLUDED.nb_votes,
                            rank = EXCLUDED.rank
                    """, record)
                except Exception as e:
                    print(f"Failed to upsert record {record}: {str(e)}")
                    conn.rollback()
                    raise
        
            conn.commit()
            print(f"Successfully loaded {len(records)} records with rankings")
        
        except Exception as e:
            raise AirflowException(f"Failed to load anime data: {str(e)}")
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    raw_data = extract_anime_data()
    transformed_data = transform_anime_data(raw_data)
    load_anime_data(transformed_data)

"""
DAG para generar un chiste sobre un animal usando IA y enviarlo por WhatsApp
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import requests

# Configuración (URLs y modelo; secretos desde Airflow Variables)
KUBEAI_API_URL = "http://kubeai.kubeai.svc.cluster.local/openai/v1/chat/completions"
KUBEAI_MODEL = "mistral-7b-flashcards"

WAHA_API_URL = Variable.get("waha_api_url", default_var="https://waha.rendonindustries.com/api/sendText")
WAHA_CHAT_ID = Variable.get("waha_chat_id", default_var="")
WAHA_SESSION = Variable.get("waha_session", default_var="default")


def _get_kubeai_api_key():
    return Variable.get("kubeai_api_key", default_var="")


def _get_waha_api_key():
    return Variable.get("waha_api_key", default_var="")


# Lista de animales
ANIMALES = [
    "perro", "gato", "elefante", "león", "tigre", "oso", "conejo", "ratón",
    "caballo", "vaca", "cerdo", "oveja", "gallina", "pato", "pez", "tortuga",
    "mono", "jirafa", "cebra", "hipopótamo", "rinoceronte", "cocodrilo",
    "serpiente", "águila", "búho", "pingüino", "delfín", "ballena", "pulpo",
    "cangrejo", "mariposa", "abeja", "hormiga", "araña", "escarabajo"
]


def generar_prompt(**context):
    """
    Tarea 1: Elegir un animal al azar y generar el prompt
    """
    animal = random.choice(ANIMALES)
    prompt = f"Dame un chiste corto y divertido sobre un {animal}"
    
    print(f"Animal elegido: {animal}")
    print(f"Prompt generado: {prompt}")
    
    # Guardar en XCom para la siguiente tarea
    context['ti'].xcom_push(key='animal', value=animal)
    context['ti'].xcom_push(key='prompt', value=prompt)
    
    return {
        'animal': animal,
        'prompt': prompt
    }


def llamar_modelo_ia(**context):
    """
    Tarea 2: Llamar al modelo de IA para obtener el chiste
    """
    # Obtener el prompt de la tarea anterior
    ti = context['ti']
    prompt = ti.xcom_pull(task_ids='generar_prompt', key='prompt')
    animal = ti.xcom_pull(task_ids='generar_prompt', key='animal')
    
    print(f"Llamando al modelo de IA con el prompt: {prompt}")
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_kubeai_api_key()}"
    }
    
    data = {
        "model": KUBEAI_MODEL,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 500
    }
    
    try:
        # Hacer la request
        response = requests.post(
            KUBEAI_API_URL,
            headers=headers,
            json=data,
            timeout=120  # 2 minutos de timeout
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            if "choices" in result and len(result["choices"]) > 0:
                chiste = result["choices"][0]["message"]["content"]
                print(f"Chiste obtenido: {chiste}")
                
                # Guardar en XCom para la siguiente tarea
                context['ti'].xcom_push(key='chiste', value=chiste)
                context['ti'].xcom_push(key='animal', value=animal)
                
                return {
                    'chiste': chiste,
                    'animal': animal
                }
            else:
                raise Exception("No se obtuvo respuesta del modelo")
        else:
            error_msg = f"Error al llamar al modelo: {response.status_code} - {response.text}"
            print(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.Timeout:
        raise Exception("Timeout al llamar al modelo de IA")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error de conexión: {e}")


def enviar_mensaje_waha(**context):
    """
    Tarea 3: Enviar el chiste por WhatsApp usando WAHA
    """
    # Obtener el chiste y el animal de las tareas anteriores
    ti = context['ti']
    chiste = ti.xcom_pull(task_ids='llamar_modelo_ia', key='chiste')
    animal = ti.xcom_pull(task_ids='llamar_modelo_ia', key='animal')
    
    if not chiste:
        raise Exception("No se obtuvo el chiste de la tarea anterior")
    
    # Preparar el mensaje
    mensaje = f"🐾 Chiste sobre {animal}:\n\n{chiste}"
    
    print(f"Enviando mensaje a {WAHA_CHAT_ID}: {mensaje}")
    
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": _get_waha_api_key()
    }
    
    data = {
        "chatId": WAHA_CHAT_ID,
        "text": mensaje,
        "session": WAHA_SESSION
    }
    
    try:
        # Enviar el mensaje
        response = requests.post(
            WAHA_API_URL,
            headers=headers,
            json=data,
            timeout=60
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        # WAHA puede devolver 200 (OK) o 201 (Created) como códigos de éxito
        if response.status_code in [200, 201]:
            result = response.json()
            print(f"✅ Mensaje enviado exitosamente: {result}")
            return result
        else:
            error_msg = f"Error al enviar mensaje: {response.status_code} - {response.text}"
            print(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error de conexión al enviar mensaje: {e}")


# Definir el DAG
# Nota: Los volúmenes para KubernetesExecutor se configuran a nivel global
# en el Helm chart, no en el DAG individual
# Schedule desde Airflow Variable; si no existe, None (solo ejecución manual)
# Crear en Admin > Variables: key=chiste_animal_dag_schedule, value=ej. "0 9 * * *" (cron)
CHISTE_ANIMAL_SCHEDULE = Variable.get("chiste_animal_dag_schedule", default_var=None)

with DAG(
    dag_id='chiste_animal_dag',
    description='Genera un chiste sobre un animal usando IA y lo envía por WhatsApp',
    schedule=CHISTE_ANIMAL_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ia', 'whatsapp', 'chiste', 'animal'],
) as dag:
    
    # Tarea 1: Generar prompt (elegir animal)
    tarea_generar_prompt = PythonOperator(
        task_id='generar_prompt',
        python_callable=generar_prompt,
    )
    
    # Tarea 2: Llamar al modelo de IA
    tarea_llamar_modelo = PythonOperator(
        task_id='llamar_modelo_ia',
        python_callable=llamar_modelo_ia,
    )
    
    # Tarea 3: Enviar mensaje por WAHA
    tarea_enviar_mensaje = PythonOperator(
        task_id='enviar_mensaje_waha',
        python_callable=enviar_mensaje_waha,
    )
    
    # Definir el orden de ejecución
    tarea_generar_prompt >> tarea_llamar_modelo >> tarea_enviar_mensaje

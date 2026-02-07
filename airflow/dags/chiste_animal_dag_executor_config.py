"""
DAG de prueba: mismo flujo que chiste_animal_dag pero usando executor_config
para que algunas tasks corran en un pod con imagen Docker distinta (Python ligera).

OPCIÓN 2: executor_config con pod_override para cambiar la imagen del pod.

Importante: Con PythonOperator la imagen del pod DEBE tener Airflow instalado
(el worker ejecuta código del DAG dentro de Airflow). Por eso:
- Si usas python:3.12-slim tal cual, la task fallará (no tiene Airflow).
- Para que funcione: usa apache/airflow:3.0.2 (como abajo) o construye una
  imagen custom FROM python:3.12-slim + pip install apache-airflow y ponla en
  EXECUTOR_CONFIG_IMAGE.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from kubernetes.client import models as k8s
import random
import requests

# Configuración (secretos desde Airflow Variables: kubeai_api_key, waha_api_key, waha_chat_id)
KUBEAI_API_URL = "http://kubeai.kubeai.svc.cluster.local/openai/v1/chat/completions"
KUBEAI_MODEL = "mistral-7b-flashcards"

WAHA_API_URL = Variable.get("waha_api_url", default_var="https://waha.rendonindustries.com/api/sendText")
WAHA_CHAT_ID = Variable.get("waha_chat_id", default_var="")
WAHA_SESSION = Variable.get("waha_session", default_var="default")


def _get_kubeai_api_key():
    return Variable.get("kubeai_api_key", default_var="")


def _get_waha_api_key():
    return Variable.get("waha_api_key", default_var="")


# Imagen para pods que usan executor_config.
# Con PythonOperator la imagen DEBE tener Airflow (el worker ejecuta airflow.sdk.execution_time.execute_workload).
# Usar apache/airflow con la misma versión que el chart. Para una "ligera" custom:
#   FROM apache/airflow:3.0.2
#   RUN pip install --no-cache-dir tu-libreria-extra
EXECUTOR_CONFIG_IMAGE = "apache/airflow:3.0.2"

# executor_config: override del pod para usar otra imagen (y opcionalmente recursos, etc.)
# El contenedor debe llamarse "base" (requisito del KubernetesExecutor)
EXECUTOR_CONFIG_LIGHT_IMAGE = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image=EXECUTOR_CONFIG_IMAGE,
                    image_pull_policy="IfNotPresent",
                )
            ]
        )
    )
}

# Lista de animales
ANIMALES = [
    "perro", "gato", "elefante", "león", "tigre", "oso", "conejo", "ratón",
    "caballo", "vaca", "cerdo", "oveja", "gallina", "pato", "pez", "tortuga",
    "mono", "jirafa", "cebra", "hipopótamo", "rinoceronte", "cocodrilo",
    "serpiente", "águila", "búho", "pingüino", "delfín", "ballena", "pulpo",
    "cangrejo", "mariposa", "abeja", "hormiga", "araña", "escarabajo"
]


def generar_prompt(**context):
    """Tarea 1: Elegir un animal al azar y generar el prompt."""
    animal = random.choice(ANIMALES)
    prompt = f"Dame un chiste corto y divertido sobre un {animal}"
    print(f"Animal elegido: {animal}")
    print(f"Prompt generado: {prompt}")
    context["ti"].xcom_push(key="animal", value=animal)
    context["ti"].xcom_push(key="prompt", value=prompt)
    return {"animal": animal, "prompt": prompt}


def llamar_modelo_ia(**context):
    """Tarea 2: Llamar al modelo de IA para obtener el chiste."""
    ti = context["ti"]
    prompt = ti.xcom_pull(task_ids="generar_prompt", key="prompt")
    animal = ti.xcom_pull(task_ids="generar_prompt", key="animal")
    print(f"Llamando al modelo de IA con el prompt: {prompt}")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_kubeai_api_key()}",
    }
    data = {
        "model": KUBEAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 500,
    }
    try:
        response = requests.post(
            KUBEAI_API_URL,
            headers=headers,
            json=data,
            timeout=120,
        )
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            if "choices" in result and len(result["choices"]) > 0:
                chiste = result["choices"][0]["message"]["content"]
                print(f"Chiste obtenido: {chiste}")
                context["ti"].xcom_push(key="chiste", value=chiste)
                context["ti"].xcom_push(key="animal", value=animal)
                return {"chiste": chiste, "animal": animal}
            raise Exception("No se obtuvo respuesta del modelo")
        raise Exception(f"Error al llamar al modelo: {response.status_code} - {response.text}")
    except requests.exceptions.Timeout:
        raise Exception("Timeout al llamar al modelo de IA")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error de conexión: {e}")


def enviar_mensaje_waha(**context):
    """Tarea 3: Enviar el chiste por WhatsApp usando WAHA."""
    ti = context["ti"]
    chiste = ti.xcom_pull(task_ids="llamar_modelo_ia", key="chiste")
    animal = ti.xcom_pull(task_ids="llamar_modelo_ia", key="animal")
    if not chiste:
        raise Exception("No se obtuvo el chiste de la tarea anterior")
    mensaje = f"🐾 Chiste sobre {animal}:\n\n{chiste}"
    print(f"Enviando mensaje a {WAHA_CHAT_ID}: {mensaje}")
    headers = {"Content-Type": "application/json", "X-Api-Key": _get_waha_api_key()}
    data = {"chatId": WAHA_CHAT_ID, "text": mensaje, "session": WAHA_SESSION}
    try:
        response = requests.post(WAHA_API_URL, headers=headers, json=data, timeout=60)
        print(f"Status Code: {response.status_code}\nResponse: {response.text}")
        if response.status_code in [200, 201]:
            print("✅ Mensaje enviado exitosamente")
            return response.json()
        raise Exception(f"Error al enviar mensaje: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error de conexión al enviar mensaje: {e}")


CHISTE_ANIMAL_SCHEDULE = Variable.get("chiste_animal_dag_schedule", default_var=None)

with DAG(
    dag_id="chiste_animal_dag_executor_config",
    description="Mismo flujo que chiste_animal_dag pero con executor_config para imagen custom por task",
    schedule=CHISTE_ANIMAL_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ia", "whatsapp", "chiste", "animal", "executor_config"],
) as dag:
    # Tarea 1: con executor_config para que el pod use EXECUTOR_CONFIG_IMAGE
    tarea_generar_prompt = PythonOperator(
        task_id="generar_prompt",
        python_callable=generar_prompt,
        executor_config=EXECUTOR_CONFIG_LIGHT_IMAGE,
    )

    # Tarea 2: sin executor_config (usa la imagen por defecto del chart)
    tarea_llamar_modelo = PythonOperator(
        task_id="llamar_modelo_ia",
        python_callable=llamar_modelo_ia,
    )

    # Tarea 3: con executor_config para usar la misma imagen custom
    tarea_enviar_mensaje = PythonOperator(
        task_id="enviar_mensaje_waha",
        python_callable=enviar_mensaje_waha,
        executor_config=EXECUTOR_CONFIG_LIGHT_IMAGE,
    )

    tarea_generar_prompt >> tarea_llamar_modelo >> tarea_enviar_mensaje

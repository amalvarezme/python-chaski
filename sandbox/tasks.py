from celery import Celery
from chaski.utils import transport
from chaski.utils import backend

# Configuración de la aplicación Celery
app = Celery(
    'example',
    broker='chaski://127.0.0.1:65433',  # URL del broker de Redis
    backend='chaski://127.0.0.1:65433',  # URL del backend de resultados
)


# Definición de una tarea simple
@app.task
def add(x, y):
    return x + y

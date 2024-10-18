from kombu import transport

# from kombu import Queue
from kombu.transport.virtual import Transport, Channel
from chaski.streamer_sync import ChaskiStreamerSync

from queue import Empty

import base64
import json

# Definimos un tópico constante
CHASKI_TOPIC = "celery_tasks"


class ChaskiChannel(Channel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.producer = ChaskiStreamerSync(
            # port=8511,
            name='Producer',
            paired=True,
        )
        self.producer.connect('ChaskiStreamer@127.0.0.1:65433')

        self.consumer = None

    def _new_queue(self, queue, **kwargs):
        # El objeto `queue` es una instancia de `Queue`, de Kombu
        print(f"Creando cola lógica: {queue}")
        # No es necesario crear el tópico en Kafka; los mensajes se manejan en `KAFKA_TOPIC`

    def _delete(self, queue, **kwargs):
        # No se puede eliminar un tópico de Kafka mediante la API de Kafka
        print(f"No se puede eliminar la cola lógica: {queue}")

    def _put(self, queue, message, **kwargs):
        # Publicar el mensaje en el tópico constante de Kafka, con los detalles de la cola lógica en los metadatos
        print(
            f"Publicando mensaje en el tópico '{CHASKI_TOPIC}' para la cola lógica '{queue}'"
        )
        # Añadimos el nombre de la cola como un campo en el mensaje para que se pueda distinguir
        message_with_metadata = {
            'queue': queue,  # Usamos `queue.name` para identificar la cola lógica
            'body': message['body'],
        }

        message['body'] = json.dumps(message['body']).encode('utf-8')

        self.producer.push(CHASKI_TOPIC, message_with_metadata)

    def _get(self, queue, timeout=None):
        # Si no existe el consumidor, lo creamos
        if self.consumer is None:
            self.consumer = ChaskiStreamerSync(
                # port=8512,
                name='Consumer',
                subscriptions=[CHASKI_TOPIC],
                # root=True,
                paired=True,
            )
            self.consumer.connect('ChaskiStreamer@127.0.0.1:65433')
            print(f"Consumidor inicializado para el tópico {CHASKI_TOPIC}")

        try:
            print(f"Reading...")
            incoming_message = next(self.consumer.message_stream(timeout=10))
            print("Received message:", incoming_message)

            body = json.loads(incoming_message.data['body'].decode('utf-8'))

            return {
                'body': body,  # Asegúrate de que 'body' esté en los datos
                'properties': {
                    'delivery_tag': 'incoming_message.delivery_tag',
                },
                'delivery_info': {
                    'routing_key': queue,  # Puede ser opcional dependiendo de tu implementación
                },
            }

        except Exception as e:
            print(e)
            raise Empty()

        # return {
        #     'body': "incoming_message.data['body']",
        #     'properties': {},
        #     'delivery_info': {},
        # }

    def close(self):
        """"""
        pass
        # Cerrar el productor y el consumidor al cerrar el canal
        # self.producer.close()
        # if self.consumer is not None:
        #     self.consumer.close()


class ChaskiTransport(Transport):
    Channel = ChaskiChannel
    default_port = 65433

    def driver_version(self):
        return 'chaski'


transport.TRANSPORT_ALIASES.update(
    {'chaski': 'chaski.utils.transport:ChaskiTransport'}
)

from celery.backends.base import BaseBackend
from celery.app import backends


class ChaskiBackend(BaseBackend):
    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        self._results = {}

    def _store_result(
        self, task_id, result, state, traceback=None, request=None, **kwargs
    ):
        print('STORING!!!')
        # Almacenar el resultado en un diccionario en memoria
        self._results[task_id] = {
            'result': result,
            'state': state,
            'traceback': traceback,
        }
        return result

    def _get_task_meta_for(self, task_id):
        # Recuperar el resultado del diccionario
        meta = self._results.get(task_id, None)
        if meta:
            return {
                'task_id': task_id,
                'result': meta['result'],
                'state': meta['state'],
                'traceback': meta['traceback'],
                'status': meta['state'],
            }
        else:
            # Si no existe el resultado, devolver 'PENDING'
            # return {'status': 'PENDING', 'result': None}

            return {
                'task_id': task_id,
                'result': 'Resultado simulado',
                'state': 'SUCCESS',
                'traceback': None,
                'status': 'SUCCESS',
            }

    def cleanup(self):
        # Limpiar los resultados (opcional)
        self._results.clear()


backends.BACKEND_ALIASES['chaski'] = 'chaski.utils.backend:ChaskiBackend'

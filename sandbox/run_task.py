from tasks import add


# Ejecutar la tarea de manera asíncrona
result = add.delay(4, 6)

# Obtener el resultado
print("Tarea enviada. Esperando resultado...")
print("Resultado:", result.get(timeout=10))

result

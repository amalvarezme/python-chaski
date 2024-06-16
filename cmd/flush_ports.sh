#!/bin/bash
# Configurar el rango de puertos que utiliza tu aplicación de nodos
START_PORT=65432
END_PORT=65532

# Función para cerrar conexiones en un puerto específico
close_connections() {
    local port=$1
    echo "Limpiando conexiones en el puerto $port..."

    # Obtener los PIDs de las conexiones activas en el puerto especificado
    pids=$(lsof -i :$port -t)

    if [ -z "$pids" ]; then
        echo "No hay conexiones activas en el puerto $port."
    else
        # Cerrar todas las conexiones activas
        for pid in $pids; do
            echo "Cerrando conexión con PID $pid..."
            kill -9 $pid
        done
    fi
}

# Recorrer todos los puertos en el rango y cerrar las conexiones
for port in $(seq $START_PORT $END_PORT); do
    close_connections $port
done

echo "Limpieza de nodos completada."

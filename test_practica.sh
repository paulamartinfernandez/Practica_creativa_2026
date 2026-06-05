#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

OK="✅"
FAIL="❌"
WARN="⚠️"

echo "   TEST FUNCIONAMIENTO"

# 1. Contenedores corriendo
echo "--- 1. Contenedores Docker ---"
for service in kafka mongo cassandra minio flask spark-master spark-worker spark-prediction; do
    status=$(docker inspect -f '{{.State.Status}}' $service 2>/dev/null)
    if [ "$status" = "running" ]; then
        echo -e "${OK} ${GREEN}$service está corriendo${NC}"
    else
        echo -e "${FAIL} ${RED}$service NO está corriendo (status: $status)${NC}"
    fi
done
echo ""

# 2. Cassandra - distancias
echo "--- 2. Distancias en Cassandra ---"
count=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null | grep -E "^[[:space:]]*[0-9]" | tr -d ' ')
if [ "$count" -gt 0 ] 2>/dev/null; then
    echo -e "${OK} ${GREEN}$count distancias en Cassandra${NC}"
else
    echo -e "${FAIL} ${RED}No hay distancias en Cassandra${NC}"
fi

# 3. Cassandra - predicciones
echo "--- 3. Predicciones en Cassandra ---"
pred=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.flight_delay_ml_response;" 2>/dev/null | grep -E "^[[:space:]]*[0-9]" | tr -d ' ')
if [ "$pred" -gt 0 ] 2>/dev/null; then
    echo -e "${OK} ${GREEN}$pred predicciones guardadas en Cassandra${NC}"
else
    echo -e "${WARN} ${YELLOW}No hay predicciones en Cassandra (haz una predicción primero)${NC}"
fi
echo ""

# 4. MinIO - datos de entrenamiento
echo "--- 4. MinIO - Datos de entrenamiento ---"
data=$(docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin -q && mc ls local/flights-data/flights/" 2>/dev/null)
if echo "$data" | grep -q "jsonl.bz2"; then
    echo -e "${OK} ${GREEN}Fichero de datos encontrado en MinIO${NC}"
else
    echo -e "${FAIL} ${RED}No hay datos de entrenamiento en MinIO${NC}"
fi

# 5. MinIO - modelos
echo "--- 5. MinIO - Modelos entrenados ---"
models=$(docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin -q && mc ls local/models/" 2>/dev/null)
if echo "$models" | grep -q "bin"; then
    echo -e "${OK} ${GREEN}Modelos encontrados en MinIO${NC}"
else
    echo -e "${FAIL} ${RED}No hay modelos en MinIO${NC}"
fi
echo ""

# 6. Kafka topics
echo "--- 6. Kafka topics ---"
topics=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)
if echo "$topics" | grep -q "flight-delay-ml-request"; then
    echo -e "${OK} ${GREEN}Topic flight-delay-ml-request existe${NC}"
else
    echo -e "${FAIL} ${RED}Topic flight-delay-ml-request NO existe${NC}"
fi
if echo "$topics" | grep -q "flight-delay-ml-response"; then
    echo -e "${OK} ${GREEN}Topic flight-delay-ml-response existe${NC}"
else
    echo -e "${FAIL} ${RED}Topic flight-delay-ml-response NO existe${NC}"
fi
echo ""

# 7. Flask responde
echo "--- 7. Flask web ---"
http_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/flights/delays/predict_kafka 2>/dev/null)
if [ "$http_code" = "200" ]; then
    echo -e "${OK} ${GREEN}Flask responde en http://localhost:5001 (HTTP $http_code)${NC}"
else
    echo -e "${FAIL} ${RED}Flask no responde (HTTP $http_code)${NC}"
fi

# 8. WebSocket disponible
echo "--- 8. WebSockets ---"
ws_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/socket.io/?EIO=4\&transport=polling 2>/dev/null)
if [ "$ws_code" = "200" ]; then
    echo -e "${OK} ${GREEN}WebSockets disponibles${NC}"
else
    echo -e "${FAIL} ${RED}WebSockets no disponibles (HTTP $ws_code)${NC}"
fi
echo ""

# 9. Spark master
echo "--- 9. Spark cluster ---"
spark_ui=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 2>/dev/null)
if [ "$spark_ui" = "200" ]; then
    echo -e "${OK} ${GREEN}Spark Master UI disponible en http://localhost:8080${NC}"
else
    echo -e "${WARN} ${YELLOW}Spark Master UI no disponible en puerto 8080${NC}"
fi
echo ""

echo "================================================"
echo "   Tests completados"
echo "================================================"

# Práctica Big Data 2026 - Predicción de Retraso de Vuelos
**Autora:** Paula Martín Fernández  
**Entorno de desarrollo:** Windows Subsystem for Linux (WSL) - Ubuntu  

Este repositorio contiene la arquitectura completa y dockerizada para el entrenamiento, almacenamiento, procesamiento en streaming y predicción en tiempo real de retrasos de vuelos en base a un modelo predictivo RandomForest.

---

## Instrucciones de Despliegue Inicial

1. Acceder a la interfaz de WSL en Windows y situarse en la carpeta de la práctica:
   ```bash
   cd ~/practica_creativa

```
<img width="813" height="51" alt="image" src="https://github.com/user-attachments/assets/6d0a4571-20b0-4a19-8b34-36ef141b3a8a" />

2. Construir las imágenes de los contenedores que componen la arquitectura:
```bash
docker compose build

```
<img width="952" height="593" alt="image" src="https://github.com/user-attachments/assets/a5cd86f1-055c-4e25-8a35-bcc26876876e" />


3. Levantar todo el escenario dockerizado en segundo plano:
```bash
docker compose up -d

```


4. **Comprobación esencial del entrenamiento:** Previo a intentar realizar cualquier predicción desde la interfaz web, se debe asegurar que el contenedor encargado del entrenamiento en batch haya finalizado correctamente la carga y generación de los modelos:
```bash
docker logs spark-training -f

```


*Debe esperarse a que finalice con código de salida `0` o que indique que los artefactos y el modelo se han guardado exitosamente.*

---

## Comprobaciones Técnicas

A continuación, se detallan las instrucciones y comandos necesarios para auditar el cumplimiento estricto de cada uno de los hitos obligatorios de la práctica:

### 1: Almacenamiento de datos de entrenamiento en Lakehouse (Iceberg + MinIO/S3)

* **Requisito:** Los datos de entrenamiento deben ser almacenados en HDFS o S3/MinIO usando Iceberg como Data Lakehouse.
* **Comprobación:** Para verificar que los datos han sido depositados correctamente bajo el formato de tablas Apache Iceberg dentro de nuestro bucket de MinIO, se comprueba accediendo a la interfaz gráfica de MinIO a través del navegador web en:
```bash
http://localhost:9001
```
<img width="951" height="948" alt="image" src="https://github.com/user-attachments/assets/8652866a-7e65-4ee3-990f-b24f20b00ada" />


### 2: Persistencia y lectura de distancias en Cassandra

* **Requisito:** Modificar el código necesario para que las distancias sean almacenadas en Cassandra y que sean leídas desde esa BBDD en lugar de MongoDB.
* **Comprobación:** Una vez levantada la arquitectura, se puede interactuar directamente con la CLI de Cassandra (`cqlsh`) para comprobar que los datos referentes a las distancias de los vuelos entre aeropuertos han sido migrados y se encuentran disponibles para lectura:
```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM agile_data_science.origin_dest_distances LIMIT 5;"
```
<img width="1239" height="305" alt="image" src="https://github.com/user-attachments/assets/f0ef40d9-64f5-4335-8c60-ad9179c41e16" />

### 3: Comunicación en tiempo real con WebSockets (Flask y Kafka) y almacenamiento final en Cassandra

* **Requisito:** El resultado de la predicción se debe escribir en un topic de Kafka, persistirse en Cassandra, y la aplicación web debe consumirlo en tiempo real desde Kafka utilizando WebSockets en lugar de hacer Polling tradicional.
* **Comprobación en Kafka (Tráfico de Topics):**
Para monitorizar en vivo cómo viajan las peticiones (`flight_delay_predict_request`) y las respuestas calculadas en streaming por Spark (`flight-delay-ml-response`), se ejecuta el siguiente consumidor de consola de Kafka mientras se solicita una predicción en la web:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic flight-delay-ml-request --from-beginning

```
<img width="1214" height="473" alt="image" src="https://github.com/user-attachments/assets/d87d8516-12ab-45ed-923a-932dc1ac1ac3" />

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic flight-delay-ml-response --from-beginning

```
<img width="1202" height="498" alt="image" src="https://github.com/user-attachments/assets/b94a1b97-1f46-4ae2-b505-40f9ace785e8" />


* **Comprobación en Cassandra (Persistencia del histórico):**
Para asegurar que tras recibir el evento de Kafka, la predicción también queda correctamente persistida de forma definitiva en Cassandra, se lanza la siguiente consulta:
```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM agile_data_science.flight_delay_ml_response LIMIT 5;"

```
<img width="1216" height="509" alt="image" src="https://github.com/user-attachments/assets/aaa0b42f-0d0c-4419-9cc1-fe68e8b029b2" />


* **Comprobación visual de WebSockets:**
Al realizar una predicción desde el formulario web (`http://localhost:5000`), el estado cambiará dinámicamente de `Waiting for prediction...` a `Done!`.
<img width="943" height="421" alt="image" src="https://github.com/user-attachments/assets/a29892a5-5f99-4ff4-8a46-ea9bf92dbb14" />

<img width="959" height="467" alt="image" src="https://github.com/user-attachments/assets/df61405e-dcc4-4bc5-b858-55a8f481e14b" />
Esta actualización se realiza de manera asíncrona mediante eventos bidireccionales (`Socket.IO`). Se puede verificar analizando el código del lado del cliente y servidor:

```bash
grep -n "socket\|io\|prediction" resources/web/templates/flight_delays_predict_kafka.html
```
<img width="951" height="580" alt="image" src="https://github.com/user-attachments/assets/1c011683-f05d-45a7-9198-c9a8e3a8b7d0" />




### 4: Lectura y escritura del entrenamiento desde el Lakehouse

* **Requisito:** El proceso de entrenamiento batch de Spark debe leer los datos iniciales directamente desde el Lakehouse desplegado (Hito 1) y almacenar los modelos resultantes en la misma estructura del Lakehouse.
* **Comprobación:** Revisando el código del script de entrenamiento o inspeccionando los logs persistidos dentro de los Workers de Spark, se puede validar que las rutas de entrada y salida apuntan al esquema del catálogo de Iceberg con el protocolo `s3a://` o localizaciones del almacén configurado:
```bash
grep -n "iceberg\|warehouse\|s3a" resources/train_spark_mllib_model.py

```
<img width="949" height="485" alt="image" src="https://github.com/user-attachments/assets/0142ff72-03f1-45a4-95c4-a08b33786df8" />



### HITO 5: Dockerización y despliegue integrado con Docker Compose

* **Requisito:** Lograr el funcionamiento integral de la práctica aislando cada uno de los servicios en contenedores independientes coordinados a través de un archivo `docker-compose.yml`.
* **Comprobación:** Para comprobar que todos los servicios requeridos (Flask, Spark Master, Spark Workers, Kafka, Cassandra, MongoDB, MinIO) se encuentran levantados y saludables en la misma red virtual, se ejecuta:
```bash
docker compose ps

```
<img width="1208" height="706" alt="image" src="https://github.com/user-attachments/assets/ddff465a-e5c3-4faf-b372-7eeeb6803565" />

### Mejoras: Observabilidad con Prometheus y Grafana

* **Requisito:** Mejoras a nivel de despliegue, observabilidad, visualización y optimización.
* **Descripción:** Se ha añadido un stack de monitorización completo con Prometheus para la recolección de métricas y Grafana para su visualización en tiempo real. Se monitoriza el tráfico de los topics de Kafka (`flight-delay-ml-request` y `flight-delay-ml-response`).

* **Comprobación:** Acceder a las interfaces web:
  - Prometheus: `http://localhost:9090`
    <img width="1892" height="879" alt="image" src="https://github.com/user-attachments/assets/6620f9d8-1c44-47a4-806f-e3af5ca22c04" />

  - Grafana: `http://localhost:3000` (usuario: `admin`, contraseña: `admin`)

* **Verificar que Prometheus recoge métricas de Kafka:**
```bash
curl -s "http://localhost:9090/api/v1/query?query=kafka_topic_partition_current_offset" | python3 -m json.tool
```

En Grafana se puede visualizar el número de mensajes procesados en cada topic en tiempo real creando un panel con la métrica `kafka_topic_partition_current_offset`.

```

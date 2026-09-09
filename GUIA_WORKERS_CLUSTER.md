# Guía de Ejecución de PySpark con Cluster Standalone (Master y Workers)

Esta guía detalla el paso a paso en consola para levantar un cluster de Spark con **Master y Workers**, monitorear los recursos desde la Web UI y ejecutar el procesamiento distribuido de los datasets.

---

## 1. Arquitectura del Cluster

```
                        +----------------------------+
                        |        Spark Master        |
                        | (spark://localhost:7077)   |
                        |      Web UI: Port 8080     |
                        +--------------+-------------+
                                       |
                   +-------------------+-------------------+
                   |                                       |
         +---------v---------+                   +---------v---------+
         |     Worker 1      |                   |     Worker 2      |
         | (2 Cores, 2GB RAM)|                   | (2 Cores, 2GB RAM)|
         +-------------------+                   +-------------------+
```

---

## 2. Paso a Paso en la Consola Linux

### Paso 1: Iniciar el Spark Master
1. Abre tu terminal en Linux y navega a la carpeta de Spark:
   ```bash
   cd ~/spark
   ```
2. Arranca el servicio Master:
   ```bash
   ./sbin/start-master.sh
   ```
3. Verifica la interfaz web del Master abriendo en tu navegador:
   ```text
   http://localhost:8080
   ```
   *(Verás en la parte superior la URL del master, normalmente `spark://localhost:7077` o `spark://usuario-VirtualBox:7077`)*.

---

### Paso 2: Iniciar los Workers y conectarlos al Master

Puedes iniciar uno o varios workers con los recursos que desees asignarles:

#### Iniciar Worker 1 (con 2 cores y 2 GB de memoria):
```bash
./sbin/start-worker.sh -c 2 -m 2G spark://localhost:7077
```

#### *(Opcional)* Iniciar un Worker 2 adicional:
```bash
./sbin/start-worker.sh -c 2 -m 2G spark://localhost:7077
```

> **Verificación:** Si recargas `http://localhost:8080`, en la tabla **Workers** verás los workers registrados con estado **ALIVE**.

---

### Paso 3: Ejecutar los Scripts Distribuidos con `spark-submit`

1. Navega a la carpeta de tu proyecto:
   ```bash
   cd ~/spark_project/DM_SPARK
   ```

2. Ejecuta el procesamiento distribuido para **Préstamos Aceptados**:
   ```bash
   spark-submit --master spark://localhost:7077 trabajo_cluster_accepted.py
   ```

3. Ejecuta el procesamiento distribuido para **Préstamos Rechazados**:
   ```bash
   spark-submit --master spark://localhost:7077 trabajo_cluster_rejected.py
   ```

---

### Paso 4: Monitoreo en Tiempo Real

Mientras se ejecutan los scripts, puedes monitorear el trabajo de los workers en dos interfaces web:

* **Spark Master UI (`http://localhost:8080`):**
  * Muestra las aplicaciones en ejecución (*Running Applications*).
  * Recursos asignados a cada worker (Cores y Memoria).
* **Spark Application UI (`http://localhost:4040`):**
  * Muestra los Jobs, Stages y Tasks que cada worker está ejecutando en paralelo.

---

### Paso 5: Detener el Cluster al finalizar

Una vez completadas las pruebas, detén los servicios en orden:

```bash
cd ~/spark
./sbin/stop-worker.sh
./sbin/stop-master.sh
```

---

## 3. Resumen de Archivos Generados

* [trabajo_cluster_accepted.py](file:///c:/Users/julio/Downloads/INF_2_2026/DM/spark/expo/DM_SPARK/trabajo_cluster_accepted.py): Script con carga balanceada, particionamiento y consultas distribuidas para préstamos aceptados.
* [trabajo_cluster_rejected.py](file:///c:/Users/julio/Downloads/INF_2_2026/DM/spark/expo/DM_SPARK/trabajo_cluster_rejected.py): Script con procesamiento distribuido para préstamos rechazados.

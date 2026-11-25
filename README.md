# Reto 2: ingesta incremental por micro-batches

**Nivel:** inicial–intermedio · **Duración sugerida:** 2–3 horas  
**Modalidad:** individual o en parejas.

## Objetivo

Comprobar el procesamiento incremental de archivos nuevos con Auto Loader y una streaming table, utilizando varias actualizaciones triggered. Una actualización puede procesar uno o varios micro-batches; no se presupone una correspondencia de uno a uno.

## Resultado esperado

Tras el primer lote hay 3 eventos; tras agregar el segundo, 5; y después de ejecutar de nuevo sin agregar archivos, siguen siendo 5. Cada `evento_id` aparece una vez.

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines, Auto Loader y cómputo compatible.
- Catálogo y esquema propios o asignados; permisos para usarlos, crear streaming tables y volúmenes, leer/escribir en el volumen y ejecutar el pipeline. Crear el catálogo requiere permisos adicionales; puede prepararlo el administrador.
- Acceso a SQL Editor o notebook. El cómputo y almacenamiento pueden generar costos.
- Permisos para crear y ejecutar Lakeflow Jobs y escribir en el volumen con la identidad que ejecuta el Job.
- Haber completado el reto 1 o comprender streaming tables y ejecución triggered.

## Identificación por participante

Forma un sufijo con la primera letra del nombre y de cada uno de los dos apellidos, en minúsculas y sin tildes; por ejemplo, `dsh`. Usa tres letras ASCII (`a`–`z`). Si dos participantes tienen las mismas iniciales o una persona no puede formar tres letras, el instructor asignará a cada uno un sufijo alternativo de tres letras **único**. Confírmalo antes de crear recursos.

Todos los ejemplos usan `dsh`: sustitúyelo por tu sufijo en **cada** nombre, consulta y ruta. El catálogo `retos_sdp_dsh` es personal y puede usarse en los tres retos; el esquema `reto02_dsh`, el volumen, la carpeta y el pipeline son específicos de este reto. La separación por nombres no reemplaza permisos de Unity Catalog ni protege frente al acceso de otros usuarios.

Los pipelines existentes con nombres anteriores no se migran automáticamente: crea un pipeline nuevo con sus recursos propios; no cambies el sufijo en un pipeline compartido o ya ejecutado.

## Datos ficticios y Job

Los campos `persona_id` y `persona_alias` son identificadores y etiquetas inventados, como `DUMMY-ID-001` y `DUMMY_UCRANIA`; los nombres de países son solo alias, no representan personas reales. No se usan documentos, correos, direcciones ni otros datos personales reales. No cargues información real en este laboratorio.

1. Importa `ingesta/generador.py` como notebook de Databricks o selecciónalo desde una Git folder.
2. En **Jobs & Pipelines**, crea el Job `job_ingesta_reto02_dsh` con una tarea **Notebook** que apunte al generador y use cómputo compatible.
3. En los **parámetros del Job** configura `sufijo = dsh` y `lote = lote_01`, sustituyendo `dsh` por tu sufijo. No dupliques estos parámetros en la tarea. Guarda el Job sin programación, con concurrencia máxima de una ejecución.
4. Después de crear el volumen del paso 1, ejecuta **Run now** y espera a que termine correctamente. Para el segundo lote, usa **Run now with different parameters** y cambia únicamente `lote` a `lote_02`. Cada lote fijo se escribe una sola vez: si el archivo ya existe, el Job falla sin sobrescribirlo.

Los archivos de `datos/` son muestras de referencia equivalentes; no los subas si ya usas el Job, para no duplicar la ingesta.

## Paso a paso

1. **Prepara una entrada limpia.** El administrador crea o asigna el catálogo personal `retos_sdp_dsh`; verifica con él que no esté asignado a otra persona. Si tienes los permisos necesarios, ejecuta en SQL Editor o notebook:

   ```sql
   CREATE CATALOG IF NOT EXISTS retos_sdp_dsh;
   CREATE SCHEMA IF NOT EXISTS retos_sdp_dsh.reto02_dsh;
   CREATE VOLUME IF NOT EXISTS retos_sdp_dsh.reto02_dsh.volumen_sdp_dsh;
   ```

   Si el catálogo ya fue provisionado, omite la primera sentencia. Usa una carpeta de entrada vacía y ejecuta el Job manual con `lote = lote_01`, según la sección anterior. El notebook crea la carpeta y el primer archivo; no ejecutes aún `lote_02`.

2. **Crea el pipeline.** Crea un archivo Python en el workspace con `solucion/pipeline.py` y agrégalo como código fuente a un nuevo pipeline llamado `pipeline_reto02_dsh`. Configura catálogo `retos_sdp_dsh`, esquema `reto02_dsh`, modo **triggered**, y en **Settings → Configuration** las claves `pipelines.suffix = dsh` y `lab.source_path = /Volumes/retos_sdp_dsh/reto02_dsh/volumen_sdp_dsh/reto02_dsh/entrada_dsh`. El código rechaza un sufijo ausente o que no tenga exactamente tres letras.

3. **Primera actualización.** Ejecuta una actualización normal y espera su finalización. Ejecuta las consultas de validación de abajo. Registra el conteo: debe ser 3.

4. **Segundo lote.** Ejecuta el mismo Job con `lote = lote_02` como se indica arriba. Cuando termine correctamente, ejecuta otra actualización normal del **mismo pipeline** y espera su finalización. El conteo debe ser 5; los identificadores `e4` y `e5` deben aparecer una vez.

5. **Reejecución sin datos nuevos.** Ejecuta una tercera actualización normal, sin subir archivos. El conteo debe seguir siendo 5 y todos los identificadores deben aparecer una vez. Revisa el historial o eventos del pipeline y anota la evidencia disponible.

6. **Analiza.** Explica qué papel juega el progreso/checkpoint gestionado por el pipeline, y por qué procesar archivos una vez no equivale a eliminar eventos de negocio duplicados.

## Validación SQL

```sql
SELECT COUNT(*) AS eventos FROM retos_sdp_dsh.reto02_dsh.vuelos_bronze_dsh;

SELECT evento_id, COUNT(*) AS veces
FROM retos_sdp_dsh.reto02_dsh.vuelos_bronze_dsh
GROUP BY evento_id
ORDER BY evento_id;
```

Resultados: primera ejecución `e1`–`e3`; segunda y tercera `e1`–`e5`, cada uno con `veces = 1`.

Comprueba también los alias ficticios:

```sql
SELECT evento_id, persona_id, persona_alias
FROM retos_sdp_dsh.reto02_dsh.vuelos_bronze_dsh ORDER BY evento_id;
```

## Ingesta sintética cada 3 minutos

Después de comprobar **3 → 5 → 5**:

1. Crea otro Job `job_programado_reto02_dsh` con una tarea **Notebook** llamada `generar_datos` que use `ingesta/generador.py`.
2. En los **parámetros del Job**, configura `sufijo = dsh`, `lote = continuo` y `registros = 500`. Sustituye el sufijo; no dupliques parámetros en la tarea. `registros` solo aplica a `continuo`, acepta enteros de **1 a 500** y vale 500 por defecto. Un valor inválido hace fallar la tarea antes de escribir.
3. Añade una tarea **Pipeline** llamada `transformar_datos`, dependiente del **éxito** de `generar_datos`, y selecciona `pipeline_reto02_dsh`. Usa actualización normal, **no full refresh**.
4. En **Add trigger → Scheduled → Advanced → Show Cron Syntax**, configura la expresión Quartz `0 0/3 * * * ?` y zona horaria `UTC`: minutos 00, 03, 06, etc. Guarda la programación inicialmente **pausada**.
5. Configura concurrencia máxima **1**, desactiva **Queue** y deja los reintentos automáticos de la tarea generadora en **0**. Si una ejecución dura más de 3 minutos, las siguientes pueden omitirse mientras sigue activa; no se superponen ni se acumulan. No se garantiza una ingesta exactamente cada 3 minutos.
6. Ejecuta **Run now** para verificar permisos y la cadena Notebook → Pipeline. Después activa la programación, observa algunas ejecuciones y vuelve a **pausarla** al terminar.

Cada ejecución exitosa del generador escribe **un archivo JSON Lines nuevo con 500 eventos** (o la cantidad elegida), sin sobrescribir. Los identificadores de evento, persona y archivo se generan con UUID e índice. Las aeronaves, países y velocidades varían usando solo valores dummy; no hay conexión a fuentes reales. Los lotes fijos `lote_01` y `lote_02` no cambian.

Revisa en la salida del notebook la ruta y el número de eventos, y repite la consulta de conteo. Si completaste la prueba manual y luego ejecutas dos lotes continuos de 500, cada uno seguido de una actualización normal, debes observar **5 → 505 → 1005**. Ejecuta otra actualización del pipeline **sin ejecutar el generador**: el conteo debe mantenerse en 1005. Comprueba que cada `evento_id` aparece una vez. Una actualización puede consumir varios archivos nuevos y uno o varios micro-batches.

El límite de 500 es **por ejecución del generador**, no para el total acumulado en Delta. Los reintentos manuales pueden producir otro lote; no se garantiza deduplicación de negocio. Esta programación aproxima near real-time, sin garantía de latencia: el planificador y el cómputo pueden retrasar ejecuciones y generan costos. Este repositorio no crea ni activa Jobs automáticamente; configura y valida la programación en tu workspace.

## Entregables y evaluación

- Evidencia de las tres ejecuciones y tabla con conteos **3 → 5 → 5**.
- Resultado de la consulta por identificador y breve explicación de incrementalidad.
- Aprobación: resultados esperados sin full refresh ni duplicación de archivos.
- Reflexión: ¿cómo probarías un evento duplicado que llega en un archivo distinto? Este reto no implementa deduplicación de negocio.

## Precauciones

No uses full refresh para la prueba: cambia la semántica del experimento. No sobrescribas el primer archivo ni borres checkpoints. Para repetir desde cero, crea un pipeline y una carpeta nuevos. El mismo contenido en archivos distintos puede procesarse más de una vez.

## Videos tutoriales en español

Ruta de estudio sugerida; los videos son material de apoyo, no sustituyen el paso a paso. Las duraciones son aproximadas y no están incluidas en la duración del reto. Enlaces y disponibilidad revisados el **25 de noviembre de 2026**; YouTube requiere conexión a Internet.

| Orden y video | Canal / duración | Qué estudiar y cómo aplicarlo |
|---|---|---|
| 1. [Databricks Bootcamp 2026 — E06: Autoloader de landing a bronze, Structured Streaming](https://www.youtube.com/watch?v=9Mw-S8N_Ukc) | Daniel Datashow · 78 min · español | Descubrimiento incremental de archivos, Auto Loader y su integración con Structured Streaming y Delta Lake. Relaciónalo con la prueba **3 → 5 → 5** y el progreso gestionado por el pipeline. |
| 2. [Lakeflow Jobs Parte 2: Triggers, Scheduled y File Arrival](https://www.youtube.com/watch?v=mXLtzno4tqo) | EDUCATRONICOS ISC · 14 min · español | Programación periódica de un Job. Después de aprobar la prueba manual, aplica Scheduled con el cron cada 3 minutos de esta guía; File Arrival es una alternativa, no se utiliza en este laboratorio. |

Si necesitas repasar tareas y parámetros, mira [Lakeflow Jobs en Databricks: Tasks, dependencias y parámetros](https://www.youtube.com/watch?v=CggUSoJ3fDQ), de **EDUCATRONICOS ISC** (33 min, español), recomendado también en el reto 1.

**Compatibilidad y seguridad:** Auto Loader con Structured Streaming en un notebook ayuda a entender la ingesta, pero aquí se ejecuta dentro de SDP. Usa `from pyspark import pipelines as dp`, el código de `solucion/pipeline.py`, Unity Catalog y `/Volumes/`. No copies checkpoints manuales, full refresh, credenciales ni datasets externos del video. Conserva el progreso gestionado por el pipeline, el generador dummy, tu sufijo y el límite de 500 registros por ejecución. Una actualización puede procesar varios micro-batches y no deduplica eventos de negocio automáticamente.

**Comprobación de estudio:** explica por qué actualizar sin archivos nuevos mantiene el conteo y por qué un evento repetido en otro archivo es un problema distinto.

## Referencias

- [Conceptos de pipelines](https://docs.databricks.com/aws/en/ldp/concepts)
- [Tutorial oficial con Auto Loader](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started)
- [Parámetros de configuración](https://docs.databricks.com/aws/en/ldp/parameters)
- [Tareas notebook en Jobs](https://docs.databricks.com/aws/en/jobs/tasks/notebook)
- [Tareas pipeline en Jobs](https://docs.databricks.com/aws/en/jobs/tasks/pipeline)
- [Programar Jobs](https://docs.databricks.com/aws/en/jobs/scheduled)

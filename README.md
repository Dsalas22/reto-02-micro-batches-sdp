# Reto 2: ingesta incremental por micro-batches con Data Vault

**Nivel:** inicial–intermedio · **Duración sugerida:** 2–3 horas  
**Modalidad:** individual o en parejas.

## Objetivo

Comprobar el procesamiento incremental de archivos nuevos con Auto Loader y varias actualizaciones triggered, y ver cómo un modelo **Data Vault** en Plata absorbe duplicados y correcciones: el **hub** conserva la primera carga de cada llave (SCD tipo 1) y el **satélite** guarda la historia de cambios (SCD tipo 2). Las capas son fijas: Bronce `raw_data.bronze`, Plata `data_vault.plata`, Oro `ods.gold`. Una actualización puede procesar uno o varios micro-batches; no se presupone una correspondencia de uno a uno.

## Resultado esperado

| Paso | Bronce | Hub | Satélite (filas / vigentes) |
|---|---:|---:|---:|
| `lote_01` | 3 | 3 | 3 / 3 |
| `lote_02` | 5 | 5 | 5 / 5 |
| Actualización sin archivos nuevos | 5 | 5 | 5 / 5 |
| `lote_03` (e5 duplicado, e4 corregido a 420) | 7 | 5 | 6 / 5 |

Al final, `e4` tiene 2 versiones en el satélite (400 cerrada y 420 vigente) y `e5` una sola. Oro (`ods.gold.r02_vuelos_por_aeronave_dsh`): A 2 eventos (promedio 225), B 2 (325), C 1 (420).

## Arquitectura y tablas

| Capa | Objeto (sufijo `dsh`) | Tipo | Contenido |
|---|---|---|---|
| Aterrizaje | `/Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto02/entrada` | Carpeta en volumen compartido | Archivos JSON del generador |
| Bronce | `raw_data.bronze.r02_vuelos_raw_dsh` | Streaming table (Auto Loader) | Todo lo que llega, incluidos duplicados, + `archivo_origen`, `fecha_archivo`, `fecha_ingesta` |
| Staging | `r02_stg_vuelos_dsh` | Vista temporal privada | Hash key, hashdiff, `fecha_carga`, `fuente_registro` |
| Plata | `data_vault.plata.r02_hub_vuelo_dsh` | Streaming table con AUTO CDC (SCD1) | Una fila por `evento_id`: la primera carga |
| Plata | `data_vault.plata.r02_sat_vuelo_dsh` | Streaming table con AUTO CDC (SCD2) | Historia de atributos; `__END_AT` nulo = versión vigente |
| Oro | `ods.gold.r02_vuelos_por_aeronave_dsh` | Vista materializada | Eventos y velocidad promedio por aeronave, solo versiones vigentes |

## Data Vault en pocas palabras

- **Hub:** lista única de llaves de negocio (`evento_id`), con su hash key `hk_vuelo` = SHA-256 de la llave normalizada. Si la llave vuelve a llegar, el hub no cambia: conserva la **primera** `fecha_carga` y `fuente_registro`.
- **Satélite:** atributos que pueden cambiar (aeronave, país, velocidad, persona). El **hashdiff** es el SHA-256 de esos atributos normalizados: si llega igual, no se crea versión nueva; si cambia, se cierra la versión anterior (`__END_AT`) y se abre otra.
- **SCD1 / SCD2:** SCD1 guarda un solo valor por llave; SCD2 guarda todas las versiones con su periodo de validez (`__START_AT`, `__END_AT`).
- **AUTO CDC** ordena las llegadas por `fecha_carga` (cuándo llegó el archivo). En el hub se usa esa fecha en negativo para que gane la carga más antigua.

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines, Auto Loader y cómputo compatible.
- No creas catálogos, esquemas ni volúmenes: el instructor ya preparó las capas fijas, el volumen compartido y los permisos (ver la sección siguiente).
- Acceso a SQL Editor o notebook. El cómputo y almacenamiento pueden generar costos.
- Permisos para crear y ejecutar Lakeflow Jobs y escribir en el volumen con la identidad que ejecuta el Job.
- Haber completado el reto 1 o comprender streaming tables y ejecución triggered.

## Preparación del instructor (una vez)

Los catálogos `raw_data`, `data_vault` y `ods` y los esquemas `raw_data.bronze`, `data_vault.plata` y `ods.gold` ya existen. El instructor crea el volumen compartido y otorga los permisos una sola vez; sirven para los tres retos. Sustituye `participantes` por el grupo de participantes (recomendado) o por un correo:

```sql
CREATE VOLUME IF NOT EXISTS raw_data.bronze.landing_retos_sdp;

GRANT USE CATALOG ON CATALOG raw_data   TO `participantes`;
GRANT USE CATALOG ON CATALOG data_vault TO `participantes`;
GRANT USE CATALOG ON CATALOG ods        TO `participantes`;

GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA raw_data.bronze  TO `participantes`;
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA data_vault.plata TO `participantes`;
GRANT USE SCHEMA, CREATE MATERIALIZED VIEW, SELECT ON SCHEMA ods.gold TO `participantes`;

GRANT READ VOLUME, WRITE VOLUME ON VOLUME raw_data.bronze.landing_retos_sdp TO `participantes`;
```

**Advertencia: el volumen es compartido.** Con `WRITE VOLUME` cualquier participante puede leer, escribir y borrar archivos en cualquier carpeta del volumen, no solo en la suya. Cada persona debe trabajar únicamente en `landing_retos_sdp/<su_sufijo>/` y nunca borrar archivos ajenos. Usa solo datos dummy. Con `SELECT` en los esquemas, los participantes también pueden consultar las tablas de los demás; cada quien es dueño solo de las tablas que crea su pipeline. Los participantes necesitan además permiso para crear pipelines (serverless o con una política de clúster) y Jobs.

## Identificación por participante

Forma un sufijo con la primera letra del nombre y de cada uno de los dos apellidos, en minúsculas y sin tildes; por ejemplo, `dsh`. Usa tres letras ASCII (`a`–`z`). Si dos participantes tienen las mismas iniciales o una persona no puede formar tres letras, el instructor asignará a cada uno un sufijo alternativo de tres letras **único**. Confírmalo antes de crear recursos.

Todos los ejemplos usan `dsh`: sustitúyelo por tu sufijo en **cada** nombre, consulta y ruta. Todas las tablas llevan el prefijo del reto (`r02_`) y tu sufijo, por ejemplo `raw_data.bronze.r02_vuelos_raw_dsh`, para no chocar con otros retos ni con otras personas. Tu carpeta de aterrizaje es `/Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto02/entrada`; el pipeline la deriva del sufijo. La separación por nombres no reemplaza permisos de Unity Catalog ni protege frente al acceso de otros usuarios.

Los pipelines existentes con nombres anteriores no se migran automáticamente: crea un pipeline nuevo con sus recursos propios; no cambies el sufijo en un pipeline compartido o ya ejecutado.

## Datos ficticios y Job

Los campos `persona_id` y `persona_alias` son identificadores y etiquetas inventados, como `DUMMY-ID-001` y `DUMMY_UCRANIA`; los nombres de países son solo alias, no representan personas reales. No se usan documentos, correos, direcciones ni otros datos personales reales. No cargues información real en este laboratorio.

1. Importa `ingesta/generador.py` como notebook de Databricks o selecciónalo desde una Git folder.
2. En **Jobs & Pipelines**, crea el Job `job_ingesta_reto02_dsh` con una tarea **Notebook** que apunte al generador y use cómputo compatible.
3. En los **parámetros del Job** configura `sufijo = dsh` y `lote = lote_01`, sustituyendo `dsh` por tu sufijo. No dupliques estos parámetros en la tarea. Guarda el Job sin programación, con concurrencia máxima de una ejecución.
4. Cuando el instructor confirme la preparación, ejecuta **Run now** y espera a que termine correctamente. Para los lotes siguientes, usa **Run now with different parameters** y cambia únicamente `lote` a `lote_02` y, más adelante, a `lote_03`. Si el volumen no existe o no tienes permiso, el Job falla con un mensaje que lo indica. Cada lote fijo se escribe una sola vez: si el archivo ya existe, el Job falla sin sobrescribirlo.

Los archivos de `datos/` son muestras de referencia equivalentes; no los subas si ya usas el Job, para no duplicar la ingesta.

## Paso a paso

1. **Primer lote.** Ejecuta el Job manual con `lote = lote_01`. La salida debe decir `Lote lote_01: 3 eventos escritos en /Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto02/entrada/lote_01.json`. No ejecutes aún `lote_02`.

2. **Crea el pipeline.** Crea un archivo Python en el workspace con `solucion/pipeline.py` y agrégalo como código fuente a un nuevo pipeline llamado `pipeline_reto02_dsh`. Destino por defecto: catálogo `raw_data`, esquema `bronze`; modo **triggered**. Plata y Oro se publican en `data_vault.plata` y `ods.gold` porque el código usa el nombre completo de cada tabla. En **Settings → Configuration** agrega solo `pipelines.suffix = dsh`; el código rechaza un sufijo ausente o que no tenga exactamente tres letras. Las claves opcionales `raw_catalog`, `raw_schema`, `silver_catalog`, `silver_schema`, `gold_catalog`, `gold_schema` y `landing_volume` ya tienen como valores por defecto `raw_data`, `bronze`, `data_vault`, `plata`, `ods`, `gold` y `landing_retos_sdp`.

3. **Primera actualización.** Ejecuta una actualización normal y espera su finalización. Ejecuta la validación SQL de abajo: Bronce 3, hub 3, satélite 3.

4. **Segundo lote.** Ejecuta el Job con `lote = lote_02`. Cuando termine, ejecuta otra actualización normal del **mismo pipeline**: 5 / 5 / 5; `e4` y `e5` aparecen una vez.

5. **Reejecución sin datos nuevos.** Ejecuta una tercera actualización normal, sin generar archivos. Los conteos siguen en 5 / 5 / 5. Revisa el historial o eventos del pipeline y anota la evidencia.

6. **Duplicado y corrección.** Ejecuta el Job con `lote = lote_03` (trae `e5` idéntico y `e4` con velocidad 420) y una actualización normal. Bronce sube a 7 porque guarda todo lo que llega; el hub sigue en 5 porque las llaves ya existían; el satélite pasa a 6 filas: `e5` no cambia (mismo hashdiff) y `e4` tiene una versión nueva.

7. **Analiza.** Explica qué papel juega el progreso/checkpoint gestionado por el pipeline, por qué procesar cada archivo una vez no evita duplicados de negocio en Bronce y cómo el hub y el hashdiff del satélite sí los resuelven en Plata.

## Validación SQL

```sql
-- Conteos por capa (repite después de cada actualización)
SELECT
  (SELECT COUNT(*) FROM raw_data.bronze.r02_vuelos_raw_dsh)  AS bronce,
  (SELECT COUNT(*) FROM data_vault.plata.r02_hub_vuelo_dsh)  AS hub,
  (SELECT COUNT(*) FROM data_vault.plata.r02_sat_vuelo_dsh)  AS sat_filas,
  (SELECT COUNT(*) FROM data_vault.plata.r02_sat_vuelo_dsh WHERE __END_AT IS NULL) AS sat_vigentes;

-- Cuántas veces llegó cada evento a Bronce
SELECT evento_id, COUNT(*) AS veces
FROM raw_data.bronze.r02_vuelos_raw_dsh
GROUP BY evento_id ORDER BY evento_id;

-- Historia del satélite unida al hub
SELECT h.evento_id, s.velocidad_kmh, s.hashdiff, s.__START_AT, s.__END_AT
FROM data_vault.plata.r02_hub_vuelo_dsh h
JOIN data_vault.plata.r02_sat_vuelo_dsh s USING (hk_vuelo)
ORDER BY h.evento_id, s.__START_AT;

-- El hub conserva la primera carga
SELECT evento_id, fecha_carga, fuente_registro
FROM data_vault.plata.r02_hub_vuelo_dsh ORDER BY evento_id;

-- Oro: versiones vigentes por aeronave
SELECT * FROM ods.gold.r02_vuelos_por_aeronave_dsh ORDER BY aeronave_id;
```

Resultados después de `lote_03`: conteos 7 / 5 / 6 / 5; en Bronce `e4` y `e5` aparecen 2 veces y el resto una; en la historia, `e4` tiene la versión 400 con `__END_AT` lleno y la versión 420 vigente; en el hub, `e4` y `e5` siguen apuntando a `lote_02.json`; Oro: A 2 / 225, B 2 / 325, C 1 / 420.

Nota: en el satélite, las columnas que no forman parte del hashdiff (`fecha_carga`, `fuente_registro`) se actualizan con la última llegada sin crear versión nueva; `__START_AT` conserva el inicio de la versión.

## Ingesta sintética cada 3 minutos

Después de comprobar la tabla de resultados esperados (hasta `lote_03`):

1. Crea otro Job `job_programado_reto02_dsh` con una tarea **Notebook** llamada `generar_datos` que use `ingesta/generador.py`.
2. En los **parámetros del Job**, configura `sufijo = dsh`, `lote = continuo` y `registros = 500`. Sustituye el sufijo; no dupliques parámetros en la tarea. `registros` solo aplica a `continuo`, acepta enteros de **1 a 500** y vale 500 por defecto. Un valor inválido hace fallar la tarea antes de escribir.
3. Añade una tarea **Pipeline** llamada `transformar_datos`, dependiente del **éxito** de `generar_datos`, y selecciona `pipeline_reto02_dsh`. Usa actualización normal, **no full refresh**.
4. En **Add trigger → Scheduled → Advanced → Show Cron Syntax**, configura la expresión Quartz `0 0/3 * * * ?` y zona horaria `UTC`: minutos 00, 03, 06, etc. Guarda la programación inicialmente **pausada**.
5. Configura concurrencia máxima **1**, desactiva **Queue** y deja los reintentos automáticos de la tarea generadora en **0**. Si una ejecución dura más de 3 minutos, las siguientes pueden omitirse mientras sigue activa; no se superponen ni se acumulan. No se garantiza una ingesta exactamente cada 3 minutos.
6. Ejecuta **Run now** para verificar permisos y la cadena Notebook → Pipeline. Después activa la programación, observa algunas ejecuciones y vuelve a **pausarla** al terminar.

Cada ejecución exitosa del generador escribe **un archivo JSON Lines nuevo con 500 eventos** (o la cantidad elegida), sin sobrescribir. Los identificadores de evento, persona y archivo se generan con UUID e índice. Las aeronaves, países y velocidades varían usando solo valores dummy; no hay conexión a fuentes reales. Los lotes fijos `lote_01`, `lote_02` y `lote_03` no cambian.

Revisa en la salida del notebook la ruta y el número de eventos, y repite la consulta de conteo. Si completaste la prueba manual y luego ejecutas dos lotes continuos de 500, cada uno seguido de una actualización normal, debes observar en Bronce **7 → 507 → 1007**, en el hub **5 → 505 → 1005** y en el satélite **6 → 506 → 1006** filas (1005 vigentes). Ejecuta otra actualización del pipeline **sin ejecutar el generador**: los conteos no cambian. Los eventos continuos tienen identificadores únicos, por eso cada uno crea una llave nueva en el hub. Una actualización puede consumir varios archivos nuevos y uno o varios micro-batches.

El límite de 500 es **por ejecución del generador**, no para el total acumulado en Delta. Los reintentos manuales pueden producir otro lote; no se garantiza deduplicación de negocio. Esta programación aproxima near real-time, sin garantía de latencia: el planificador y el cómputo pueden retrasar ejecuciones y generan costos. Este repositorio no crea ni activa Jobs automáticamente; configura y valida la programación en tu workspace.

## Entregables y evaluación

- Evidencia de las cuatro actualizaciones y tabla con conteos Bronce / hub / satélite: **3/3/3 → 5/5/5 → 5/5/5 → 7/5/6**.
- Resultado de la historia del satélite para `e4` y `e5`, y breve explicación de incrementalidad, SCD1 y SCD2.
- Aprobación: resultados esperados y Oro A 2/225, B 2/325, C 1/420, sin full refresh ni duplicación de archivos.
- Reflexión: ¿qué pasaría en el satélite si `e4` volviera a llegar con velocidad 400?

## Precauciones

No uses full refresh para la prueba: cambia la semántica del experimento. No sobrescribas el primer archivo ni borres checkpoints. Para repetir desde cero, crea un pipeline y una carpeta nuevos. El mismo contenido en archivos distintos se guarda más de una vez en Bronce; Plata lo absorbe con el hub y el hashdiff.

## Videos tutoriales en español

Ruta de estudio sugerida; los videos son material de apoyo, no sustituyen el paso a paso. Las duraciones son aproximadas y no están incluidas en la duración del reto. Enlaces y disponibilidad revisados el **25 de noviembre de 2026**; YouTube requiere conexión a Internet.

| Orden y video | Canal / duración | Qué estudiar y cómo aplicarlo |
|---|---|---|
| 1. [Databricks Bootcamp 2026 — E06: Autoloader de landing a bronze, Structured Streaming](https://www.youtube.com/watch?v=9Mw-S8N_Ukc) | Daniel Datashow · 78 min · español | Descubrimiento incremental de archivos, Auto Loader y su integración con Structured Streaming y Delta Lake. Relaciónalo con la prueba **3 → 5 → 5 → 7** en Bronce y el progreso gestionado por el pipeline. |
| 2. [Lakeflow Jobs Parte 2: Triggers, Scheduled y File Arrival](https://www.youtube.com/watch?v=mXLtzno4tqo) | EDUCATRONICOS ISC · 14 min · español | Programación periódica de un Job. Después de aprobar la prueba manual, aplica Scheduled con el cron cada 3 minutos de esta guía; File Arrival es una alternativa, no se utiliza en este laboratorio. |

Si necesitas repasar tareas y parámetros, mira [Lakeflow Jobs en Databricks: Tasks, dependencias y parámetros](https://www.youtube.com/watch?v=CggUSoJ3fDQ), de **EDUCATRONICOS ISC** (33 min, español), recomendado también en el reto 1.

**Compatibilidad y seguridad:** Auto Loader con Structured Streaming en un notebook ayuda a entender la ingesta, pero aquí se ejecuta dentro de SDP. Usa `from pyspark import pipelines as dp`, el código de `solucion/pipeline.py`, Unity Catalog y `/Volumes/`. No copies checkpoints manuales, full refresh, credenciales ni datasets externos del video. Conserva el progreso gestionado por el pipeline, el generador dummy, tu sufijo y el límite de 500 registros por ejecución. Una actualización puede procesar varios micro-batches; Bronce no deduplica eventos de negocio, la deduplicación ocurre en el hub y el satélite.

**Comprobación de estudio:** explica por qué actualizar sin archivos nuevos mantiene el conteo, por qué un evento repetido en otro archivo llega dos veces a Bronce y cómo el hub y el hashdiff evitan duplicarlo en Plata.

## Referencias

- [Conceptos de pipelines](https://learn.microsoft.com/es-es/azure/databricks/ldp/concepts)
- [Tutorial oficial con Auto Loader](https://learn.microsoft.com/es-es/azure/databricks/getting-started/data-pipeline-get-started)
- [Parámetros de configuración](https://learn.microsoft.com/es-es/azure/databricks/ldp/parameters)
- [Tareas notebook en Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/tasks/notebook)
- [Tareas pipeline en Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/tasks/pipeline)
- [Programar Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/scheduled)
- [AUTO CDC: SCD tipo 1 y tipo 2](https://learn.microsoft.com/es-es/azure/databricks/ldp/cdc)

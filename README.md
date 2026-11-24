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
- Haber completado el reto 1 o comprender streaming tables y ejecución triggered.

## Identificación por participante

Forma un sufijo con la primera letra del nombre y de cada uno de los dos apellidos, en minúsculas y sin tildes; por ejemplo, `dsh`. Usa tres letras ASCII (`a`–`z`). Si dos participantes tienen las mismas iniciales o una persona no puede formar tres letras, el instructor asignará a cada uno un sufijo alternativo de tres letras **único**. Confírmalo antes de crear recursos.

Todos los ejemplos usan `dsh`: sustitúyelo por tu sufijo en **cada** nombre, consulta y ruta. El catálogo `retos_sdp_dsh` es personal y puede usarse en los tres retos; el esquema `reto02_dsh`, el volumen, la carpeta y el pipeline son específicos de este reto. La separación por nombres no reemplaza permisos de Unity Catalog ni protege frente al acceso de otros usuarios.

Los pipelines existentes con nombres anteriores no se migran automáticamente: crea un pipeline nuevo con sus recursos propios; no cambies el sufijo en un pipeline compartido o ya ejecutado.

## Paso a paso

1. **Prepara una entrada limpia.** El administrador crea o asigna el catálogo personal `retos_sdp_dsh`; verifica con él que no esté asignado a otra persona. Si tienes los permisos necesarios, ejecuta en SQL Editor o notebook:

   ```sql
   CREATE CATALOG IF NOT EXISTS retos_sdp_dsh;
   CREATE SCHEMA IF NOT EXISTS retos_sdp_dsh.reto02_dsh;
   CREATE VOLUME IF NOT EXISTS retos_sdp_dsh.reto02_dsh.volumen_sdp_dsh;
   ```

   Si el catálogo ya fue provisionado, omite la primera sentencia. En Catalog Explorer crea la carpeta vacía `reto02_dsh/entrada_dsh` dentro del volumen. Sube **únicamente** `datos/lote_01.json`. No subas todavía el segundo lote ni la carpeta completa de datos.

2. **Crea el pipeline.** Crea un archivo Python en el workspace con `solucion/pipeline.py` y agrégalo como código fuente a un nuevo pipeline llamado `pipeline_reto02_dsh`. Configura catálogo `retos_sdp_dsh`, esquema `reto02_dsh`, modo **triggered**, y en **Settings → Configuration** las claves `pipelines.suffix = dsh` y `lab.source_path = /Volumes/retos_sdp_dsh/reto02_dsh/volumen_sdp_dsh/reto02_dsh/entrada_dsh`. El código rechaza un sufijo ausente o que no tenga exactamente tres letras.

3. **Primera actualización.** Ejecuta una actualización normal y espera su finalización. Ejecuta las consultas de validación de abajo. Registra el conteo: debe ser 3.

4. **Segundo lote.** Sube `datos/lote_02.json` a la misma carpeta, sin modificar ni volver a subir el primer archivo. Ejecuta otra actualización normal del **mismo pipeline** y espera su finalización. El conteo debe ser 5; los identificadores `e4` y `e5` deben aparecer una vez.

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

## Entregables y evaluación

- Evidencia de las tres ejecuciones y tabla con conteos **3 → 5 → 5**.
- Resultado de la consulta por identificador y breve explicación de incrementalidad.
- Aprobación: resultados esperados sin full refresh ni duplicación de archivos.
- Reflexión: ¿cómo probarías un evento duplicado que llega en un archivo distinto? Este reto no implementa deduplicación de negocio.

## Precauciones

No uses full refresh para la prueba: cambia la semántica del experimento. No sobrescribas el primer archivo ni borres checkpoints. Para repetir desde cero, crea un pipeline y una carpeta nuevos. El mismo contenido en archivos distintos puede procesarse más de una vez.

## Referencias

- [Conceptos de pipelines](https://docs.databricks.com/aws/en/ldp/concepts)
- [Tutorial oficial con Auto Loader](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started)
- [Parámetros de configuración](https://docs.databricks.com/aws/en/ldp/parameters)

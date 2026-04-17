**Proyecto ETL - ODS 15: Vida de Ecosistemas Terrestres**

Por:

-Jose David Santa

-Valentina Morales

-Brayan Stiven Tigreros

**Objetivo del proyecto**

El propósito de este proyecto es diseñar e implementar un pipeline ETL en entorno de producción que integre y analice datos de incautaciones de fauna silvestre en los departamentos de Risaralda y Caldas, Colombia, junto con información externa sobre el estado de conservación de las especies. Este sistema se desarrolla en el marco de los Objetivos de Desarrollo Sostenible (ODS 15: Vida de Ecosistemas Terrestres), con el fin de generar conocimiento que contribuya a la protección de la biodiversidad y la lucha contra el tráfico ilegal de especies y la extinción de ellas.
Desde su concepción, el pipeline está diseñado para trabajar con múltiples fuentes de datos que se complementan entre sí. Por un lado, se cuenta con datasets en formato CSV que registran las incautaciones de fauna silvestre realizadas por autoridades en Risaralda y Caldas. Por otro lado, se integra información proveniente de la API de Global Biodiversity Information Facility (GBIF), la cual proporciona el estado de conservación de las especies según estándares internacionales como la Lista Roja de la Unión Internacional para la Conservación de la Naturaleza.
La integración de estas fuentes permite enriquecer significativamente el análisis, ya que no solo se identifican las especies incautadas, sino también su nivel de riesgo a nivel global. Para lograr esto, el pipeline realiza procesos de extracción, limpieza, transformación, validación y carga en un Data Warehouse en MySQL, bajo un modelo dimensional que facilita el análisis. Ademas de su respectiva orquestación en airflow.
El desarrollo de este sistema responde a la necesidad de convertir datos aislados en información útil para la toma de decisiones, en el contexto Colombiano donde el tráfico ilegal de especies representa uno de los delitos ambientales más rentables y dañinos. Esta actividad impacta negativamente la biodiversidad, incrementa el riesgo de extinción de especies y altera los ecosistemas, especialmente en regiones altamente biodiversas como Risaralda y Caldas..
A partir de la integración de ambas fuentes de datos, el pipeline busca relacionar las especies registradas en incautaciones en estos departamentos con su nivel de riesgo a nivel mundial, con el fin de analizar cómo el tráfico ilegal está afectando a las especies en estos contextos específicos. Esta integración permite habilitar análisis que no eran posibles con el dataset original, entre ellos: identificar qué proporción de los animales incautados corresponde a especies amenazadas o en peligro crítico, determinar qué autoridades interceptan más especies de alto riesgo ecológico, y analizar la relación entre la frecuencia de incautaciones y el nivel de amenaza de las especies.
Finalmente, los resultados esperados incluyen la generación de dashboards interactivos en Power BI que permitan identificar patrones, tendencias y riesgos asociados al tráfico de fauna silvestre. Esto facilita la toma de decisiones por parte de entidades ambientales y contribuye al cumplimiento de los objetivos de conservación de la biodiversidad y mitigar o eliminar su riesgo de extinción.

**Objetivos específicos**

- Integrar los datos de incautaciones de fauna silvestre con la información de estado de conservación proveniente de la API de GBIF.
- Diseñar y ejecutar el proceso de ETL que incluyan limpieza y validación de datos, asegurando la calidad e integridad de la información antes de su almacenamiento y utilización.
- Identificar patrones entre las incautaciones y su el nivel de riesgo de las especies 
- Orquestar el pipeline mediante herramientas como Apache Airflow para garantizar la automatización, trazabilidad y ejecución periódica de los procesos, validando el éxito de cada uno de los tasks.
- Implementar un Data Warehouse en MySQL bajo un modelo dimensional que permita consultas eficientes orientadas al análisis de incautaciones y nivel de amenaza de las especies.
- Desarrollar un dashboard que permitan visualizar tendencias, distribuciones y relaciones clave para la toma de decisiones.

**Fuentes de datos**

***Dataset incautaciones.csv***
El archivo incautaciones.csv es extraido de datos.gov.co el cual contiene aproximadamente 12,836 registros con 10 atributos: año del evento con un rango de fechas de 2008 a 2022, departamento, municipio, lugar del decomiso, situación (INCAUTACIÓN, ENTREGA VOLUNTARIA o HALLAZGO), autoridad que intervino, tipo de especie, nombre común, nombre científico y cantidad de individuos.

***API gbif_raw.csv***
La API de Global Biodiversity Information Facility (GBIF), la cual proporciona información de la jerarquía taxonómica completa de cada especie: reino, filo, clase, orden y familia. Por otro lado, la categoría de amenaza IUCN (LC, NT, VU, EN, CR, NE, DD), que indica el nivel de riesgo de extinción según la Lista Roja internacional. Esta API fue seleccionada debido a que nos proporciona una lista larga de especies y en especial evalúa la amenaza en la que se encuentran estas especies algo muy importante para desarrollar nuestro ODS (ODS 15: Vida de Ecosistemas Terrestres), dándonos la oportunidad de conocer el nivel de amenaza en la que se encuentra y identificar la relación entre su nivel de riego con las incautaciones registradas en estos dos departamentos de Colombia que son muy ricos en biodiversidad.

**Profiling summary**

***Information general del dataset incautaciones.csv***

- Number of columns: 10
- Number of rows: 12,836
- Total memory used (MB): 6.5 
- Duplicados totales: 3924

| column_name | Data type | Missing values | % of missing values | Cardinality | Basic Statistics | Notes |
|---|---:|---:|---:|---:|---|---|
| **año** | Float64 | 0 | 0.00 | … | Count = 12836<br>Mean = 2015<br>Min = 2008<br>Max = 2021 | Representa el año de la incautación |
| **Departamento** | Object | 0 | 0 | 2 | … | Representa el departamento de la incautación |
| **Municipio** | Object | 31 | 0.2 | 16 | … | Representa el municipio de la incautación |
| **Lugar Decomiso** | Object | 0 | 0.00 | 483 | … | Representa el lugar de la incautación |
| **Situacion** | Object | 0 | 0.00 | 3 | … | Representa la situación en la que se realizó la incautación |
| **Autoridad que incauto** | Object | 34 | 0.3 | 6 | … | Representa la autoridad que realizó la incautación |
| **nom tipo especie** | Object | 10 | 0.1 | 12 | … | Representa el nombre de la especie incautada |
| **Nombre comun** | Object | 738 | 5.7 | 632 | … | Representa el nombre común del individuo incautado |
| **Nombre cientifico** | Object | 703 | 5.5 | 568 | … | Representa el nombre científico del individuo incautado |
| **Cantidad** | Int64 | 0 | 0 | … | Count = 12836<br>Mean = 1.237<br>Min = 1<br>Max = 150 | Representa la cantidad de individuos incautados |

---

***Information general de gbif_raw.csv***

- Number of columns: 13
- Number of rows: 567
- Total memory used (MB): 368.9
- Duplicados totales: 0

| column_name | Data type | Missing values | % of missing values | Cardinality | Basic Statistics | Notes |
|---|---|---:|---:|---:|---|---|
| **nombre_cientifico_original** | Object | 0 | 0.00 | 567 | … | Representa el nombre científico original del dataset `incautaciones.csv` |
| **nombre_cientifico_normalizado** | Object | 0 | 0 | 541 | … | Representa el nombre científico normalizado del dataset `incautaciones.csv` |
| **nombre_cientifico_gbif** | Object | 41 | 7.23 | 441 | … | Representa el nombre científico que se extrajo de GBIF |
| **usage_key** | float64 | 41 | 7.23 | … | Count = 567<br>Mean = 3.68<br>Min = 1<br>Max = 1.11 | Es el identificador interno del taxón en GBIF |
| **reino** | Object | 41 | 7.23 | 3 | … | Representa el reino al que pertenece |
| **filo** | Object | 43 | 7.58 | 6 | … | Representa el filo al que pertenece |
| **clase** | Object | 48 | 8.47 | 15 | … | Representa la clase a la que pertenece |
| **orden** | Object | 148 | 26.10 | 47 | … | Representa el orden al que pertenece |
| **familia** | Object | 46 | 8.11 | 133 | … | Representa la familia a la que pertenece |
| **genero** | Object | 46 | 8.11 | 285 | … | Representa el género al que pertenece |
| **estado_taxonomico** | Object | 41 | 7.23 | 4 | … | Indica el estado del nombre en la taxonomía interpretada por GBIF. Aparecen valores como `ACCEPTED`, `SYNONYM` y `DOUBTFUL` |
| **confianza_match** | Int65 | 0 | 0 | … | Count = 567<br>Mean = 89.7<br>Min = 0<br>Max = 99 | Representa el nivel de confianza del emparejamiento entre el nombre consultado y el taxón encontrado por GBIF |
| **categoria_iucn** | Object | 87 | 15.34 | 8 | … | Es la categoría de amenaza IUCN |

## Modelo Dimensional
**Definición de la granularidad**

| Dimensión              | Atributo 1              | Atributo 2      | Atributo 3        |
|------------------------|------------------------|-----------------|------------------|
| Especie dimensión      | Nom tipo especie       | Nombre común   | Nombre científico |
| Ubicación dimensión    | Departamento           | Municipio      | Lugar decomiso    |
| Autoridad dimensión    | Autoridad que incautó  |                 |                  |
| Tiempo dimensión       | Año                    |                 |                  |
| Fact Table             | Situación              | Cantidad        |                  |


Un registro por evento de incautación o entrega de fauna silvestre, identificado por la combinación de año, ubicación (departamento + municipio + lugar), especie (tipo + nombre común + nombre científico) y autoridad interviniente.

**Decisiones del Esquema Estrella**

La fact table fact_incautaciones contiene dos medidas: 

cantidad (individuos) y situacion (tipo de evento). 

Se mantuvo en la fact table y no como dimensión porque solo tiene 3 valores distintos y varía por evento individual. 

Las cuatro dimensiones son dim_tiempo (año), dim_ubicacion (departamento, municipio, lugar), dim_especie (tipo, nombre común, nombre científico) y dim_autoridad (entidad interviniente).

Todos los surrogate keys se generan con reset_index() + 1 en pandas antes de la carga.

lugar_decomiso se integró dentro de dim_ubicacion por formar parte de la jerarquía geográfica del evento.

Imagen del Esquema Estrella:

<img width="1187" height="792" alt="image" src="https://github.com/user-attachments/assets/aa1fe6eb-c739-4937-b72b-f2c863d80f48" />

**Lógica ETL**

En la fase de extracción se lee el CSV con pandas usando separador coma y encoding UTF-8, asignando los nombres de columna definidos. 

En la transformación se corrige el año multiplicando por 1000, se imputan los nulos con el valor DESCONOCIDO, se estandariza todo el texto con strip y upper, se construyen las cuatro dimensiones eliminando duplicados y se genera la fact table mediante merges secuenciales con cada dimensión para obtener las llaves foráneas. 

En la carga, las dimensiones se insertan usando INSERT IGNORE con tabla temporal para evitar duplicados, y la fact table aplica un anti-join contra las llaves ya existentes en el DW para garantizar idempotencia del pipeline.

**Supuestos de Calidad de Datos**

Los nulos en nombre común y científico se interpretan como registros sin identificación taxonómica completa al momento de la incautación. Los nulos en autoridad corresponden a eventos sin registro de la entidad interviniente. El año en formato 2.008 se considera un problema de serialización del CSV original y no un error en los datos. La capitalización inconsistente entre registros (por ejemplo "Centro Rehabilitacion AVES RAPACES" vs "CARDER") se normaliza a mayúsculas para garantizar consistencia en las dimensiones.

**Cómo ejecutar el proyecto**

Primero instala las dependencias

    pip install -r requirements.txt

Luego crea la base de datos en MySQL ejecutando el archivo **incautaciones.sql** para crear las tablas. 

En main.py actualiza las tres rutas: log_file, target_file y data_path según tu entorno local.

En load.py reemplaza usuario y contraseña en la cadena de conexión de SQLAlchemy.

Finalmente ejecuta el main.py y verás en consola el preview tabular de cada dimensión y la fact table, además del log de cada fase.

La consola deberia dar esta salida:

<img width="589" height="71" alt="image" src="https://github.com/user-attachments/assets/7307a195-eaec-403e-bed6-8d3360db8d64" />

En MySQL Workbench, comprueba que se cargaron los registros, abre un SQL script y escribe el comando:

    SELECT * FROM incautaciones_dw.fact_incautaciones;

Al ejecutarlo, deberias ver la tabla de hechos juntos con las llaves foraneas de las dimensiones, tambien puedes comprobar si se cargaron los registros de las dimensiones

    SELECT * FROM incautaciones_dw.dim_nombreDeDimension;

Por ejemplo, para la dimension autoridad:

    SELECT * FROM incautaciones_dw.dim_autoridad;

<img width="883" height="338" alt="image" src="https://github.com/user-attachments/assets/412a51d0-31c6-4646-9ae7-5b824a47ad4d" />



Ahora bien, para conectar el MySQL Workbench al Power BI necesitaremos un extra:

MySQL Connector/ODBC (version 8.x/64bit).

Esto es necesario puesto que este proyecto consume el Data Warehouse desde Power BI mediante una fuente de datos ODBC configurada con el driver de MySQL.

Creando el DSN
Abre ODBC Data Sources (64-bit) en Windows, ve a la pestaña System DSN y haz clic en Add.

Selecciona MySQL ODBC 8.0 Unicode Driver y configura los siguientes campos: 

  Data Source Name: incautaciones_dw_dsn
  
  TCP/IP: Server localhost
  
  Port: 3306
  
  User y Password: tus credenciales de MySQL
  
  Database: incautaciones_dw. 

Haz clic en Test para verificar la conectividad y luego OK para guardar.

El repositorio incluye el archivo .pbix con el reporte prearmado. Para conectarlo a tu Data Warehouse local después de clonar el proyecto, abre el archivo ubicado en projecto_etl_ods/diagrams/power_bi_kpis/kpis_incautaciones.pbix con Power BI Desktop. 

Luego ve a Inicio → Transformar Datos → Configuración de origen de datos

Selecciona la fuente ODBC, haz clic en Editar Permisos e ingresa tus credenciales de MySQL si se solicitan. 

Confirma que el DSN utilizado sea incautaciones_dw_dsn (o el que tengas creado) y refresca el dataset desde Inicio → Actualizar.

Después de refrescar, el reporte mostrará los siguientes KPIs: 

-cantidad de animales incautados por especie

-incautados por año

-proporción por situación (incautación, entrega voluntaria y hallazgo)

-incautados por autoridad

-total de individuos incautados. 

Los valores del reporte deben coincidir con el contenido de la base de datos incautaciones_dw tras ejecutar el pipeline completo con python main.py. Un ejemplo de como deberia lucir el reporte que se mostrara a continuacion.

**KPIs del Dashboard**

| Requerimientos | KPIs | Dimensiones | Valor |
|---|---|---|---|
| ¿Cuáles son los animales por especie con más individuos incautados | Cantidad de especies incautadas | Especie | Por medio de esta se puede determinar cuál es la especie que en más peligro se encuentra dentro de los departamentos debido al alto número de individuos que ya se han incautado en el momento. Este KPI entra en vigor al utilizar la opcion de filtrado por especie incluida en el dashboard. |
| ¿Cuál es la autoridad que más incautaciones ha realizado o lidera las incautaciones? | Cantidad incautada por autoridad | Autoridad | Con estos datos se puede ver a cuáles de las autoridades competentes dar más crédito o a cuáles de ellas están teniendo un rendimiento menor para implementar medidas de mejora para esperar mejores resultados de su parte. |
| ¿Cómo es la distribución de las incautaciones según su situación? | Cantidad de incautaciones según su situación | Especie | Mediante esto se pueden identificar cuál es la situación en la que mayormente se está haciendo el proceso de incautación. Esto permite identificar cómo es la tendencia de incautaciones y por cuál método optar o cuáles reforzar. |
| ¿Cómo ha evolucionado el volumen de fauna incautada entre 2008 y 2021? | Cantidad de incautaciones entre 2008 y 2021 | Tiempo | Mediante esta información se busca conocer la cantidad de fauna incautada en ese rango de años y determinar cómo ha sido su evolución a lo largo de los años. Esto permite conocer la distribución y evaluar por qué se está realizando un incremento o reducción en la cantidad, evaluando posibles causas. | 
| ¿Cuáles son los animales por nombre común que más frecuentemente se han incautado? | Cantidad incautada por nombre común del animal | Especie | Por medio de esta información se busca conocer el nombre del animal que más está siendo afectado por el proceso de tráfico de especies. Esto ayuda a generar planes de mitigación del problema como campañas de protección sobre animales afectados. |
| ¿Cuál es el total acumulado de individuos registrados en el DW? | Cantidad de individuos registrados | Especie | Mediante esto se puede conocer la cantidad de individuos que están siendo registrados y ver cómo esta problemática está afectando a los departamentos en cuestión para aplicar medidas de protección animal en las zonas. |


<img width="1439" height="809" alt="Captura de pantalla 2026-03-05 225552" src="https://github.com/user-attachments/assets/742d1e5c-5a2d-4902-95d6-f7b87eee44b9" />


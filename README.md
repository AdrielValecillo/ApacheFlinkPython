# Apache Flink - Analizador de Texto Interactivo

![Apache Flink Logo](https://flink.apache.org/img/flink-header-logo.svg)

## Descripción del Proyecto

Este proyecto implementa un analizador de texto interactivo utilizando Apache Flink y FastAPI. La aplicación permite procesar textos en español, manteniendo correctamente los acentos y caracteres especiales como "ñ". La interfaz web proporciona una forma intuitiva de interactuar con las capacidades de procesamiento de flujos de datos de Apache Flink.

## ¿Qué es Apache Flink?

Apache Flink es un framework y motor de procesamiento distribuido para cálculos con estado sobre flujos de datos ilimitados y limitados. Está diseñado para ejecutarse en todos los entornos de cluster comunes, realizar cálculos a velocidad de memoria y a cualquier escala.

Características clave de Flink:
- Procesamiento de flujos con estado
- Garantías de procesamiento exactamente una vez
- Baja latencia y alto rendimiento
- Soporte para ventanas de tiempo
- Alta disponibilidad

## Características de la Aplicación

- **Interfaz Web Interactiva**: Proporciona una interfaz de usuario moderna para interactuar con Apache Flink.
- **Soporte para Texto en Español**: Maneja correctamente acentos, diéresis y la letra "ñ".
- **Múltiples Tipos de Análisis**:
  - **Contador de Palabras**: Cuenta la frecuencia de cada palabra en el texto.
  - **Extracción de Palabras Clave**: Identifica y extrae palabras clave filtrando palabras comunes.
- **Visualización de Resultados**: Muestra los resultados en tablas y gráficos interactivos.
- **Procesamiento Flexible**: Permite analizar texto introducido directamente o archivos subidos.

## Estructura del Proyecto

```
apacheFlinkTutorial/
├── main.py                 # Aplicación FastAPI principal
├── flink_processor.py      # Procesador Apache Flink
├── word_count.py           # Implementación básica de contador de palabras
├── templates/             # Plantillas HTML
│   └── index.html          # Interfaz web interactiva
├── static/                # Archivos estáticos (CSS, JS, imágenes)
├── uploads/               # Carpeta para archivos subidos
├── requirements.txt       # Dependencias principales de Apache Flink
└── requirements-additional.txt # Dependencias adicionales (FastAPI, etc.)
```

## Requisitos

- Python 3.8 o superior
- Java 8 o superior (necesario para Apache Flink)
- Paquetes Python (incluidos en requirements.txt y requirements-additional.txt):
  - apache-flink==2.0.0
  - fastapi==0.109.2
  - uvicorn==0.27.1
  - y otras dependencias necesarias

## Instalación

1. Clona o descarga este repositorio:
   ```bash
   git clone <url-del-repositorio>
   cd apacheFlinkTutorial
   ```

2. Crea y activa un entorno virtual Python:
   ```bash
   python -m venv venv
   
   # En Windows
   .\venv\Scripts\activate
   
   # En Unix/MacOS
   source venv/bin/activate
   ```

3. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-additional.txt
   ```

## Ejecución de la Aplicación

1. Asegúrate de que el entorno virtual esté activado.

2. Inicia la aplicación con Uvicorn:
   ```bash
   uvicorn main:app --reload --port 8000
   ```

3. Abre tu navegador y visita: http://localhost:8000

## Cómo Usar la Aplicación

### Análisis de Texto

1. En la pestaña "Análisis de Texto":
   - Ingresa el texto que deseas analizar en el área de texto.
   - Selecciona el tipo de análisis (Contador de Palabras o Extracción de Palabras Clave).
   - Haz clic en "Analizar Texto".

2. Los resultados se mostrarán en el panel derecho:
   - Una tabla con las palabras y sus frecuencias.
   - Un gráfico de barras visualizando los resultados.

### Subir Archivo

1. En la pestaña "Subir Archivo":
   - Selecciona un archivo de texto (.txt, .csv, .md, etc.) para analizar.
   - Elige el tipo de análisis.
   - Haz clic en "Subir y Analizar".

2. Los resultados se mostrarán en el mismo formato que en el análisis de texto directo.

## Cómo Funciona

### Flujo de Datos

1. **Entrada de Datos**: El texto se recibe a través de la interfaz web (entrada directa o archivo).

2. **Preprocesamiento**: El texto se normaliza (conversión a minúsculas, eliminación de puntuación) manteniendo acentos y caracteres especiales del español.

3. **Procesamiento con Apache Flink**:
   - Se crea un entorno de ejecución de Flink.
   - El texto se convierte en un flujo de datos.
   - Se aplican operaciones de transformación (flat_map, key_by, sum).

4. **Recopilación de Resultados**: Los resultados se procesan y se convierten a un formato adecuado para la visualización.

5. **Visualización**: Los resultados se muestran en la interfaz web mediante tablas y gráficos.

### Componentes Clave

#### WordSplitter (flink_processor.py)

Esta clase divide el texto en palabras individuales, preservando los caracteres especiales del español:

```python
class WordSplitter(FlatMapFunction):
    def flat_map(self, line):
        # Convertir a minúsculas
        line = line.lower()
        
        # Eliminar puntuación
        for char in string.punctuation:
            line = line.replace(char, ' ')
        
        # Procesar cada palabra
        for word in words:
            if re.match(r'^[a-záéíóúüñ]+$', word, re.UNICODE):
                yield (word, 1)
```

#### KeywordExtractor (flink_processor.py)

Esta clase extrae palabras clave filtrando palabras comunes (stopwords) en español:

```python
class KeywordExtractor(FlatMapFunction):
    def __init__(self):
        # Palabras comunes a filtrar (stopwords)
        self.stopwords = set(['el', 'la', 'los', 'las', ...])
    
    def flat_map(self, line):
        # ...
        # Filtrar stopwords y palabras cortas
        keywords = [word for word in words if word not in self.stopwords and len(word) > 3]
        
        for keyword in keywords:
            yield (keyword, 1)
```

#### FlinkProcessor (flink_processor.py)

Esta clase maneja la ejecución de trabajos de Apache Flink y el procesamiento de resultados:

```python
class FlinkProcessor:
    def execute_job(self, job: TextAnalysisJob):
        # Crear entorno Flink
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # Procesar flujo de datos
        counts = text_stream \
            .flat_map(processor) \
            .key_by(lambda pair: pair[0]) \
            .sum(1)
        
        # Ejecutar y recolectar resultados
        # ...
```

#### API FastAPI (main.py)

Gestiona las solicitudes HTTP y la comunicación con el frontend:

```python
@app.post("/analyze/")
async def analyze_text(text: str = Form(...), job_type: str = Form("word_count")):
    # ...
    # Ejecutar trabajo en un hilo separado
    thread = threading.Thread(target=run_job)
    thread.start()
    # ...
```

## Ejemplos de Uso

### Ejemplo 1: Análisis de un Texto Simple

**Entrada**: "Apache Flink es una plataforma excelente para procesamiento de datos en tiempo real. Flink permite procesar flujos de datos de manera eficiente."

**Resultado del Contador de Palabras**:
```
Item         Conteo
---------------------
flink        2
procesar     2
datos        2
apache       1
plataforma   1
...
```

**Resultado de Extracción de Palabras Clave**:
```
Item         Conteo
---------------------
flink        2
procesar     2
datos        2
plataforma   1
tiempo       1
...
```

### Ejemplo 2: Análisis de un Texto con Caracteres Especiales

**Entrada**: "La España del siglo XXI está caracterizada por su diversidad cultural. Los niños aprenden rápido sobre tecnología."

**Resultado del Contador de Palabras**:
```
Item         Conteo
---------------------
españa       1
siglo        1
está         1
caracterizada 1
diversidad   1
niños        1
rápido       1
tecnología   1
...
```

## Personalización

Puedes personalizar la aplicación de varias maneras:

1. **Añadir Nuevos Tipos de Análisis**: Crea nuevas clases en `flink_processor.py` que implementen `FlatMapFunction` y añádelas al método `get_processor()` de la clase `TextAnalysisJob`.

2. **Modificar la Interfaz Web**: Edita `templates/index.html` para cambiar la apariencia o añadir nuevas funcionalidades.

3. **Ampliar la API**: Añade nuevos endpoints en `main.py` para proporcionar funcionalidades adicionales.

## Limitaciones Conocidas

- La aplicación está diseñada para procesamiento local, no para clusters distribuidos de Flink.
- El tamaño de los archivos que se pueden procesar está limitado por la memoria disponible.
- La aplicación está optimizada para el idioma español; el procesamiento de otros idiomas puede requerir ajustes.

## Contribuciones

Las contribuciones son bienvenidas. Si deseas mejorar este proyecto, puedes:

1. Crear un fork del repositorio
2. Crear una rama para tu función (`git checkout -b nueva-funcion`)
3. Hacer commit de tus cambios (`git commit -am 'Añadir nueva función'`)
4. Hacer push a la rama (`git push origin nueva-funcion`)
5. Crear un nuevo Pull Request

## Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo LICENSE para más detalles.

## Recursos Adicionales

- [Documentación oficial de Apache Flink](https://flink.apache.org/docs/stable/)
- [Documentación de FastAPI](https://fastapi.tiangolo.com/)
- [Tutorial de PyFlink](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/overview/)

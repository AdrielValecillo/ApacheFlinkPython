# Contador de Palabras con Apache Flink

### Descripción del Proyecto

Este proyecto implementa un contador de palabras utilizando Python y Apache Flink que procesa texto en español, manteniendo correctamente los acentos y caracteres especiales como "ñ". La aplicación lee texto desde un archivo y cuenta la frecuencia de cada palabra de manera insensible a mayúsculas y minúsculas.

### Características

- **Soporte para texto en español**: Maneja correctamente acentos (á, é, í, ó, ú), diéresis (ü) y la letra ñ
- **Conteo insensible a mayúsculas/minúsculas**: Todas las palabras se procesan en minúsculas
- **Fácil de usar**: Solo necesitas editar un archivo de texto para ver los resultados
- **Implementado con Apache Flink**: Utiliza el framework de procesamiento de datos distribuido para escalabilidad

### Requisitos

- Python 3.12 o compatible
- Java 17 (OpenJDK 17.0.15 o compatible)
- Paquetes Python (incluidos en requirements.txt):
  - apache-flink==2.0.0
  - py4j==0.10.9.7
  - protobuf==5.29.4
  - numpy==2.1.3
  - pandas==2.2.3
  - Y otras dependencias necesarias

### Configuración del Entorno

1. Clona o descarga este repositorio:
   ```
   git clone <url-del-repositorio>
   cd apacheFlinkTutorial
   ```

2. Crea y activa un entorno virtual Python:
   ```
   python -m venv venv
   .\venv\Scripts\activate  # En Windows
   source venv/bin/activate  # En Unix/MacOS
   ```

3. Instala las dependencias usando el archivo requirements.txt:
   ```
   pip install -r requirements.txt
   ```

### Cómo Ejecutarlo

1. Asegúrate de que el entorno virtual esté activado.

2. Crea o edita el archivo `input_text.txt` en la raíz del proyecto con el texto que deseas analizar.
   Ejemplo:
   ```
   El español tiene múltiples acentos y caracteres especiales.
   La niña está jugando con el niño en el jardín.
   ¡Qué día más bonito hace hoy!
   ```

3. Ejecuta la aplicación:
   ```
   python word_count.py
   ```

4. Observa los resultados en la consola, donde se mostrará cada palabra y su frecuencia.

### Ejemplos de Uso

#### Entrada de Ejemplo:
```
El cielo está muy azul hoy.
Las nubes blancas flotan suavemente.
El árbol más alto tiene manzanas rojas.
```

#### Salida Esperada:
```
('el', 2)
('cielo', 1)
('está', 1)
('muy', 1)
('azul', 1)
('hoy', 1)
('las', 1)
('nubes', 1)
('blancas', 1)
('flotan', 1)
('suavemente', 1)
('árbol', 1)
('más', 1)
('alto', 1)
('tiene', 1)
('manzanas', 1)
('rojas', 1)
```

### Detalles Técnicos

La aplicación utiliza la API DataStream de Apache Flink para procesar texto. Los componentes principales son:

1. **WordSplitter**: Una función FlatMapFunction personalizada que tokeniza el texto en palabras preservando los caracteres especiales del español
2. **Procesamiento de flujo de datos**: El texto se lee desde un archivo, se transforma en un flujo y se procesa
3. **Conteo de palabras**: Las palabras se agrupan por clave y se cuentan utilizando las funciones de agregación de Flink

Para más detalles, examine el archivo `word_count.py`.


import re
import sys
import os
import string
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction


class WordSplitter(FlatMapFunction):

    def flat_map(self, line):
        # Convert to lowercase for case-insensitive counting
        line = line.lower()
        
        # Remove punctuation marks but preserve letters with accents and ñ
        # We don't use unicodedata.normalize here to keep accented characters intact
        
        # Define punctuation to remove (excluding letters with accents)
        spanish_punct = ''.join(c for c in string.punctuation)
        
        # Replace punctuation with spaces
        for char in spanish_punct:
            line = line.replace(char, ' ')
        
        # Split by whitespace and filter out empty strings
        words = [word.strip() for word in line.split() if word.strip()]
        
        # Process each word
        for word in words:
            # Additional check to ensure the word contains only valid Spanish characters
            if re.match(r'^[a-záéíóúüñ]+$', word, re.UNICODE):
                yield (word, 1)


def word_count():
    # Crear el entorno de ejecución
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configurar para mostrar una salida de registro más detallada
    env.set_parallelism(1)  # Usar paralelismo de 1 para pruebas locales
    
    # Crear una ruta de archivo de entrada de muestra
    input_file = os.path.join(os.getcwd(), "input_text.txt")
    
    # Crear un archivo vacío si no existe
    if not os.path.exists(input_file):
        with open(input_file, "w", encoding="utf-8") as f:
            f.write("Bienvenido al contador de palabras con Apache Flink\n")
            f.write("Escriba texto en español para contar palabras\n")
    
    # Leer líneas desde el archivo de texto
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Crear un flujo de datos a partir de la colección de líneas
    text_stream = env.from_collection(lines)
    
    # Procesar el flujo
    counts = text_stream \
        .flat_map(WordSplitter()) \
        .key_by(lambda word_count: word_count[0]) \
        .sum(1)
    
    # Imprimir los resultados en la consola en tiempo real
    counts.print()
    
    # Ejecutar el trabajo de Flink
    env.execute("Contador de Palabras en Español")


if __name__ == '__main__':
    print("Iniciando aplicación de Contador de Palabras...")
    print("Instrucciones:")
    print("1. Edita el archivo 'input_text.txt' en este directorio")
    print("2. Añade texto en español al archivo")
    print("3. El programa contará las palabras y mostrará los resultados")
    print("4. Presiona Ctrl+C para salir")
    
    input_file = os.path.join(os.getcwd(), "input_text.txt")
    print(f"\nArchivo de entrada: {input_file}")
    
    try:
        word_count()
    except KeyboardInterrupt:
        print("\nDeteniendo la aplicación Flink...")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        print("\nConsejos de solución de problemas:")
        print("1. Asegúrate de que el archivo input_text.txt existe y se puede escribir")
        print("2. Asegúrate de que tu entorno virtual está activado")
        print("3. Intenta añadir algo de texto al archivo de entrada manualmente")
        sys.exit(1)


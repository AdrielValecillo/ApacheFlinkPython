import os
import re
import string
from typing import List, Dict, Any
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction
import json

class WordSplitter(FlatMapFunction):
    """
    Divide el texto en palabras manejando texto en español con acentos y caracteres especiales.
    Realiza procesamiento insensible a mayúsculas/minúsculas.
    """
    def flat_map(self, line):
        # Convertir a minúsculas para conteo insensible a mayúsculas/minúsculas
        line = line.lower()
        
        # Eliminar signos de puntuación pero preservar letras con acentos y ñ
        spanish_punct = ''.join(c for c in string.punctuation)
        
        # Reemplazar puntuacion con espacios
        for char in spanish_punct:
            line = line.replace(char, ' ')
        
        # Dividir por espacios en blanco y filtrar cadenas vacías
        words = [word.strip() for word in line.split() if word.strip()]
        
        # Procesar cada palabra
        for word in words:
            # Verificación adicional para asegurar que la palabra contiene solo caracteres españoles válidos
            if re.match(r'^[a-zu00e1u00e9u00edu00f3u00fau00fcu00f1]+$', word, re.UNICODE):
                yield (word, 1)



class KeywordExtractor(FlatMapFunction):
    """
    Extrae palabras clave del texto basado en frecuencia y filtrado de palabras comunes.
    """
    def __init__(self):
        # Palabras comunes a filtrar (stopwords)
        self.stopwords = set([
            'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas', 'y', 'o', 'a', 'ante', 'bajo',
            'con', 'contra', 'de', 'desde', 'en', 'entre', 'hacia', 'hasta', 'para', 'por', 'segu00fan',
            'sin', 'sobre', 'tras', 'durante', 'mediante', 'que', 'cual', 'quien', 'cuyo', 'como',
            'cuando', 'cuanto', 'donde', 'adonde', 'si', 'este', 'esta', 'estos', 'estas', 'ese',
            'esa', 'esos', 'esas', 'aquel', 'aquella', 'aquellos', 'aquellas', 'mi', 'tu', 'su',
            'mis', 'tus', 'sus', 'nuestro', 'vuestro', 'del', 'al', 'lo'
        ])
    
    def flat_map(self, line):
        # Convertir a minúsculas
        line = line.lower()
        
        # Eliminar puntuación
        for char in string.punctuation:
            line = line.replace(char, ' ')
        
        # Dividir en palabras
        words = [word.strip() for word in line.split() if word.strip()]
        
        # Filtrar stopwords y palabras cortas
        keywords = [word for word in words if word not in self.stopwords and len(word) > 3]
        
        # Emitir cada palabra clave con un valor de 1 para conteo
        for keyword in keywords:
            yield (keyword, 1)

class TextAnalysisJob:
    """
    Representa un trabajo de análisis de texto utilizando Apache Flink.
    """
    def __init__(self, job_type: str, input_file: str):
        self.job_type = job_type
        self.input_file = input_file
        self.output_file = f"{os.path.splitext(input_file)[0]}_results.json"
    
    def get_processor(self):
        """
        Obtiene la función de procesamiento adecuada según el tipo de trabajo.
        """
        if self.job_type == "word_count":
            return WordSplitter()
        elif self.job_type == "keyword_extraction":
            return KeywordExtractor()
        else:
            # Por defecto, usar contador de palabras
            return WordSplitter()

class FlinkProcessor:
    """
    Clase principal para ejecutar trabajos de Apache Flink.
    Proporciona métodos para ejecutar diferentes tipos de análisis de texto.
    """
    def __init__(self):
        pass
    
    def execute_job(self, job: TextAnalysisJob) -> List[Dict[str, Any]]:
        """
        Ejecuta un trabajo de análisis de texto con Apache Flink y devuelve los resultados.
        """
        try:
            # Crear el entorno de ejecución
            env = StreamExecutionEnvironment.get_execution_environment()
            env.set_parallelism(1)  # Usar paralelismo de 1 para pruebas locales
            
            # Leer líneas desde el archivo de texto
            with open(job.input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Crear un flujo de datos a partir de la colección de líneas
            text_stream = env.from_collection(lines)
            
            # Obtener el procesador adecuado para este trabajo
            processor = job.get_processor()
            
            # Procesar el flujo - Usamos collect() para obtener los resultados directamente
            # en lugar de intentar usar un sink personalizado
            counts = text_stream \
                .flat_map(processor) \
                .key_by(lambda pair: pair[0]) \
                .sum(1)
            
            # Ejecutar el trabajo de Flink y recolectar resultados
            job_name = f"Apache Flink - {job.job_type.replace('_', ' ').title()}"
            env.execute(job_name)
            
            # Recolectar resultados manualmente para evitar duplicados
            with open(job.input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Procesar el texto para contar palabras
            word_count = {}
            processor = job.get_processor()
            
            for line in lines:
                for word, count in processor.flat_map(line):
                    if word in word_count:
                        word_count[word] += count
                    else:
                        word_count[word] = count
            
            # Convertir al formato esperado
            results = [{"item": word, "count": count} for word, count in word_count.items()]
            
            # Ordenar resultados por conteo (descendente)
            results.sort(key=lambda x: x["count"], reverse=True)
            
            # Guardar resultados en un archivo JSON
            with open(job.output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            return results
        
        except Exception as e:
            # En caso de error, devolver información sobre el error
            error_info = {"error": str(e), "job_type": job.job_type, "input_file": job.input_file}
            return [error_info]


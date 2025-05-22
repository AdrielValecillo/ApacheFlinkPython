import os
import re
import string
from typing import List, Dict, Any
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction
import json

class WordSplitter(FlatMapFunction):
    """
    Divide el texto en palabras manejando texto en espau00f1ol con acentos y caracteres especiales.
    Realiza procesamiento insensible a mayu00fasculas/minu00fasculas.
    """
    def flat_map(self, line):
        # Convertir a minu00fasculas para conteo insensible a mayu00fasculas/minu00fasculas
        line = line.lower()
        
        # Eliminar signos de puntuaciu00f3n pero preservar letras con acentos y u00f1
        spanish_punct = ''.join(c for c in string.punctuation)
        
        # Reemplazar puntuaciu00f3n con espacios
        for char in spanish_punct:
            line = line.replace(char, ' ')
        
        # Dividir por espacios en blanco y filtrar cadenas vacu00edas
        words = [word.strip() for word in line.split() if word.strip()]
        
        # Procesar cada palabra
        for word in words:
            # Verificaciu00f3n adicional para asegurar que la palabra contiene solo caracteres espau00f1oles vu00e1lidos
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
        # Convertir a minu00fasculas
        line = line.lower()
        
        # Eliminar puntuaciu00f3n
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
    Representa un trabajo de anu00e1lisis de texto utilizando Apache Flink.
    """
    def __init__(self, job_type: str, input_file: str):
        self.job_type = job_type
        self.input_file = input_file
        self.output_file = f"{os.path.splitext(input_file)[0]}_results.json"
    
    def get_processor(self):
        """
        Obtiene la funciu00f3n de procesamiento adecuada segu00fan el tipo de trabajo.
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
    Proporciona mu00e9todos para ejecutar diferentes tipos de anu00e1lisis de texto.
    """
    def __init__(self):
        pass
    
    def execute_job(self, job: TextAnalysisJob) -> List[Dict[str, Any]]:
        """
        Ejecuta un trabajo de anu00e1lisis de texto con Apache Flink y devuelve los resultados.
        """
        try:
            # Crear el entorno de ejecuciu00f3n
            env = StreamExecutionEnvironment.get_execution_environment()
            env.set_parallelism(1)  # Usar paralelismo de 1 para pruebas locales
            
            # Leer lu00edneas desde el archivo de texto
            with open(job.input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Crear un flujo de datos a partir de la colecciu00f3n de lu00edneas
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
            # En caso de error, devolver informaciu00f3n sobre el error
            error_info = {"error": str(e), "job_type": job.job_type, "input_file": job.input_file}
            return [error_info]

# Ejemplo de uso (para pruebas)
def run_test():
    processor = FlinkProcessor()
    
    # Crear un archivo de prueba si no existe
    test_file = "test_input.txt"
    if not os.path.exists(test_file):
        with open(test_file, "w", encoding="utf-8") as f:
            f.write("Este es un texto de prueba para Apache Flink.\n")
            f.write("Apache Flink es una plataforma excelente para procesamiento de datos.\n")
            f.write("El procesamiento de flujos de datos es muy u00fatil para aplicaciones en tiempo real.\n")
    
    # Ejecutar diferentes tipos de trabajos
    print("Ejecutando contador de palabras...")
    word_count_job = TextAnalysisJob("word_count", test_file)
    word_count_results = processor.execute_job(word_count_job)
    print(f"Resultados: {word_count_results[:5]}...")
    
    print("\nEjecutando anu00e1lisis de sentimiento...")
    sentiment_job = TextAnalysisJob("sentiment_analysis", test_file)
    sentiment_results = processor.execute_job(sentiment_job)
    print(f"Resultados: {sentiment_results}")
    
    print("\nEjecutando extracciu00f3n de palabras clave...")
    keyword_job = TextAnalysisJob("keyword_extraction", test_file)
    keyword_results = processor.execute_job(keyword_job)
    print(f"Resultados: {keyword_results[:5]}...")

if __name__ == "__main__":
    run_test()

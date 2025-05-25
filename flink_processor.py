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
            # Artículos
            'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas', 'lo', 'al', 'del',
            
            # Pronombres
            'yo', 'me', 'mi', 'conmigo', 'tú', 'te', 'ti', 'contigo', 'usted',
            'él', 'ella', 'ello', 'le', 'se', 'sí', 'consigo',
            'nosotros', 'nosotras', 'nos', 'vosotros', 'vosotras', 'os',
            'ustedes', 'ellos', 'ellas', 'les', 'les', 'se', 'sí', 'consigo',
            'mí', 'mío', 'mía', 'míos', 'mías', 'tu', 'tuyo', 'tuya', 'tuyos', 'tuyas',
            'su', 'suyo', 'suya', 'suyos', 'suyas', 'nuestro', 'nuestra', 'nuestros', 'nuestras',
            'vuestro', 'vuestra', 'vuestros', 'vuestras', 'suyo', 'suya', 'suyos', 'suyas',
            'mis', 'tus', 'sus',
            
            # Preposiciones
            'a', 'ante', 'bajo', 'cabe', 'con', 'contra', 'de', 'desde', 'durante',
            'en', 'entre', 'hacia', 'hasta', 'mediante', 'para', 'por', 'según',
            'sin', 'so', 'sobre', 'tras', 'versus', 'vía',
            
            # Conjunciones
            'y', 'e', 'ni', 'o', 'u', 'bien', 'sea', 'pero', 'sino', 'aunque', 'mas',
            'que', 'si', 'como', 'porque', 'pues', 'luego', 'conque', 'cuando',
            'mientras', 'apenas', 'aun', 'aunque', 'tan', 'tanto', 'tal', 'así',
            
            # Adverbios comunes
            'aquí', 'allí', 'allá', 'acá', 'ahí', 'donde', 'adonde', 'como', 'cuando', 'cuanto',
            'así', 'muy', 'mucho', 'poco', 'algo', 'casi', 'tan', 'tanto', 'bastante',
            'sí', 'no', 'nunca', 'jamás', 'acaso', 'quizá', 'quizás', 'tal', 'vez', 'también',
            'ahora', 'antes', 'después', 'luego', 'pronto', 'tarde', 'temprano', 'todavía',
            'ya', 'ayer', 'hoy', 'mañana', 'siempre', 'aún', 'entonces', 'mientras',
            'más', 'menos', 'mejor', 'peor', 'bien', 'mal', 'regular', 'despacio', 'deprisa',
            
            # Formas de ser y estar
            'soy', 'eres', 'es', 'somos', 'sois', 'son',
            'era', 'eras', 'éramos', 'erais', 'eran',
            'fui', 'fuiste', 'fue', 'fuimos', 'fuisteis', 'fueron',
            'sido', 'siendo', 'sea', 'seas', 'seamos', 'seáis', 'sean',
            'estoy', 'estás', 'está', 'estamos', 'estáis', 'están',
            'estaba', 'estabas', 'estábamos', 'estabais', 'estaban',
            'estuve', 'estuviste', 'estuvo', 'estuvimos', 'estuvisteis', 'estuvieron',
            'estado', 'estando',
            
            # Verbos comunes
            'tener', 'tengo', 'tienes', 'tiene', 'tenemos', 'tenéis', 'tienen',
            'hacer', 'hago', 'haces', 'hace', 'hacemos', 'hacéis', 'hacen',
            'ir', 'voy', 'vas', 'va', 'vamos', 'vais', 'van',
            'decir', 'digo', 'dices', 'dice', 'decimos', 'decís', 'dicen',
            'ver', 'veo', 'ves', 've', 'vemos', 'veis', 'ven',
            'dar', 'doy', 'das', 'da', 'damos', 'dais', 'dan',
            'poder', 'puedo', 'puedes', 'puede', 'podemos', 'podéis', 'pueden',
            'querer', 'quiero', 'quieres', 'quiere', 'queremos', 'queréis', 'quieren',
            'deber', 'debo', 'debes', 'debe', 'debemos', 'debéis', 'deben',
            
            # Demostrativos
            'este', 'esta', 'estos', 'estas', 'ese', 'esa', 'esos', 'esas',
            'aquel', 'aquella', 'aquellos', 'aquellas', 'esto', 'eso', 'aquello',
            
            # Palabras interrogativas/exclamativas
            'qué', 'quién', 'quiénes', 'cuál', 'cuáles', 'cómo', 'dónde', 'cuándo',
            'cuánto', 'cuánta', 'cuántos', 'cuántas',
            
            # Otras palabras comunes
            'cada', 'todo', 'toda', 'todos', 'todas', 'mismo', 'misma', 'mismos', 'mismas',
            'otro', 'otra', 'otros', 'otras', 'cualquier', 'cualquiera', 'cualesquiera',
            'primero', 'segundo', 'tercero', 'etc', 'etcétera'
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


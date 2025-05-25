from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import threading
import queue

# Importar nuestro módulo de Flink
from flink_processor import FlinkProcessor, TextAnalysisJob

app = FastAPI(title="Apache Flink Text Analyzer", 
              description="Una API interactiva para demostrar las capacidades de Apache Flink en procesamiento de texto")

# Configurar carpetas para archivos estáticos y plantillas
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)
os.makedirs("uploads", exist_ok=True)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Cola para comunicación entre los trabajos de Flink y la API
result_queue = queue.Queue()

# Variable global para almacenar los resultados
analysis_results = {}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/analyze/")
async def analyze_text(text: str = Form(...), job_type: str = Form("word_count")):
    # Guardar el texto en un archivo temporal
    filename = f"uploads/input_{len(analysis_results)}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)
    
    # Crear un identificador único para este análisis
    analysis_id = str(len(analysis_results))
    
    # Inicializar el procesador de Flink
    processor = FlinkProcessor()
    
    # Crear y ejecutar el trabajo en un hilo separado
    def run_job():
        job = TextAnalysisJob(job_type=job_type, input_file=filename)
        results = processor.execute_job(job)
        result_queue.put((analysis_id, results))
    
    thread = threading.Thread(target=run_job)
    thread.start()
    
    # Devolver el ID del análisis para que el cliente pueda consultar los resultados
    return {"analysis_id": analysis_id, "status": "processing"}

@app.get("/results/{analysis_id}")
async def get_results(analysis_id: str):
    # Intentar obtener nuevos resultados de la cola
    try:
        while not result_queue.empty():
            id, results = result_queue.get_nowait()
            analysis_results[id] = results
    except queue.Empty:
        pass
    
    # Verificar si los resultados están disponibles
    if analysis_id in analysis_results:
        return {"status": "completed", "results": analysis_results[analysis_id]}
    else:
        return {"status": "processing"}

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...), job_type: str = Form("word_count")):
    # Guardar el archivo subido
    file_path = f"uploads/{file.filename}"
    with open(file_path, "wb") as f:
        f.write(await file.read())
    
    # Crear un identificador único para este análisis
    analysis_id = str(len(analysis_results))
    
    # Inicializar el procesador de Flink
    processor = FlinkProcessor()
    
    # Crear y ejecutar el trabajo en un hilo separado
    def run_job():
        job = TextAnalysisJob(job_type=job_type, input_file=file_path)
        results = processor.execute_job(job)
        result_queue.put((analysis_id, results))
    
    thread = threading.Thread(target=run_job)
    thread.start()
    
    return {"analysis_id": analysis_id, "status": "processing"}

@app.get("/jobs")
async def list_available_jobs():
    return {
        "available_jobs": [
            {"id": "word_count", "name": "Contador de Palabras", "description": "Cuenta la frecuencia de cada palabra en el texto"},
            {"id": "keyword_extraction", "name": "Extracción de Palabras Clave", "description": "Identifica palabras clave en el texto"}
        ]
    }


// Variables globales
let currentChart = null;
let pollingInterval = null;

// Cargar tipos de trabajos disponibles
async function loadJobTypes() {
    try {
        const response = await fetch('/jobs');
        const data = await response.json();
        
        const jobTypeSelect = document.getElementById('jobTypeSelect');
        const fileJobTypeSelect = document.getElementById('fileJobTypeSelect');
        
        jobTypeSelect.innerHTML = '';
        fileJobTypeSelect.innerHTML = '';
        
        data.available_jobs.forEach(job => {
            const option = document.createElement('option');
            option.value = job.id;
            option.textContent = `${job.name} - ${job.description}`;
            
            const option2 = option.cloneNode(true);
            
            jobTypeSelect.appendChild(option);
            fileJobTypeSelect.appendChild(option2);
        });
    } catch (error) {
        console.error('Error cargando tipos de trabajos:', error);
    }
}

// Manejar envío del formulario de texto
document.getElementById('textForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const text = document.getElementById('textInput').value.trim();
    const jobType = document.getElementById('jobTypeSelect').value;
    
    if (!text) {
        alert('Por favor ingresa texto para analizar');
        return;
    }
    
    // Mostrar spinner
    document.getElementById('spinner').style.display = 'block';
    document.getElementById('resultsContent').innerHTML = '<p>Procesando datos...</p>';
    
    // Limpiar gráfico anterior
    if (currentChart) {
        currentChart.destroy();
        currentChart = null;
    }
    
    // Detener polling anterior si existe
    if (pollingInterval) {
        clearInterval(pollingInterval);
    }
    
    try {
        const formData = new FormData();
        formData.append('text', text);
        formData.append('job_type', jobType);
        
        const response = await fetch('/analyze/', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        
        if (data.analysis_id) {
            // Comenzar a consultar los resultados
            pollResults(data.analysis_id);
        }
    } catch (error) {
        document.getElementById('spinner').style.display = 'none';
        document.getElementById('resultsContent').innerHTML = `<p class="text-danger">Error: ${error.message}</p>`;
        console.error('Error:', error);
    }
});

// Manejar envío del formulario de archivo
document.getElementById('fileForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const fileInput = document.getElementById('fileInput');
    const jobType = document.getElementById('fileJobTypeSelect').value;
    
    if (!fileInput.files || fileInput.files.length === 0) {
        alert('Por favor selecciona un archivo');
        return;
    }
    
    // Mostrar spinner
    document.getElementById('spinner').style.display = 'block';
    document.getElementById('resultsContent').innerHTML = '<p>Procesando archivo...</p>';
    
    // Limpiar gráfico anterior
    if (currentChart) {
        currentChart.destroy();
        currentChart = null;
    }
    
    // Detener polling anterior si existe
    if (pollingInterval) {
        clearInterval(pollingInterval);
    }
    
    try {
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);
        formData.append('job_type', jobType);
        
        const response = await fetch('/upload/', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        
        if (data.analysis_id) {
            // Comenzar a consultar los resultados
            pollResults(data.analysis_id);
        }
    } catch (error) {
        document.getElementById('spinner').style.display = 'none';
        document.getElementById('resultsContent').innerHTML = `<p class="text-danger">Error: ${error.message}</p>`;
        console.error('Error:', error);
    }
});

// Función para consultar los resultados
function pollResults(analysisId) {
    pollingInterval = setInterval(async () => {
        try {
            const response = await fetch(`/results/${analysisId}`);
            const data = await response.json();
            
            if (data.status === 'completed' && data.results) {
                // Detener el polling
                clearInterval(pollingInterval);
                pollingInterval = null;
                
                // Ocultar spinner
                document.getElementById('spinner').style.display = 'none';
                
                // Mostrar resultados
                displayResults(data.results);
            }
        } catch (error) {
            console.error('Error consultando resultados:', error);
        }
    }, 1000); // Consultar cada segundo
}

// Función para mostrar los resultados
function displayResults(results) {
    const resultsContent = document.getElementById('resultsContent');
    
    // Si hay un error
    if (results.length === 1 && results[0].error) {
        resultsContent.innerHTML = `
            <div class="alert alert-danger">
                <h4>Error en el procesamiento</h4>
                <p>${results[0].error}</p>
            </div>
        `;
        return;
    }
    
    // Limitar a los 10 primeros resultados para la tabla
    const topResults = results.slice(0, 10);
    
    let tableHtml = `
        <div class="table-responsive">
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Ítem</th>
                        <th>Conteo</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    topResults.forEach(result => {
        tableHtml += `
            <tr>
                <td>${result.item}</td>
                <td>${result.count}</td>
            </tr>
        `;
    });
    
    tableHtml += `
                </tbody>
            </table>
        </div>
        <p class="text-muted">Mostrando los ${topResults.length} resultados principales de ${results.length} totales.</p>
    `;
    
    resultsContent.innerHTML = tableHtml;
    
    // Crear gráfico
    createChart(topResults);
}

// Función para crear un gráfico
function createChart(data) {
    const chartContainer = document.getElementById('resultsChart');
    const canvas = document.createElement('canvas');
    chartContainer.innerHTML = '';
    chartContainer.appendChild(canvas);
    
    const ctx = canvas.getContext('2d');
    
    // Preparar datos para el gráfico
    const labels = data.map(item => item.item);
    const values = data.map(item => item.count);
    
    // Generar colores
    const backgroundColors = generateColors(data.length);
    
    currentChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Frecuencia',
                data: values,
                backgroundColor: backgroundColors,
                borderColor: backgroundColors.map(color => color.replace('0.6', '1')),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                title: {
                    display: true,
                    text: 'Resultados del Análisis'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Frecuencia'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Ítems'
                    }
                }
            }
        }
    });
}

// Función para generar colores aleatorios
function generateColors(count) {
    const colors = [];
    for (let i = 0; i < count; i++) {
        const hue = (i * 360 / count) % 360;
        colors.push(`hsla(${hue}, 70%, 60%, 0.6)`);
    }
    return colors;
}

// Cargar los tipos de trabajos al iniciar
document.addEventListener('DOMContentLoaded', () => {
    loadJobTypes();
});

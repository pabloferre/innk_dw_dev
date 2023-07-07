import openai
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY_OAI = os.environ.get('API_KEY_OAI') 
API = API_KEY_OAI
openai.api_key = API


import openai

def classify_field_names(field_names):
    prompt = "classify these field names accordingly to their similarities:"
    for field_name in field_names:
        prompt += '\n"' + field_name + '",'
    prompt = prompt[:-1]  # Remove the last comma
    prompt += '\n\n---\n\nclassification:'

    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=100,
        n=1,
        stop=None,
        temperature=0.5
    )

    classification = response.choices[0].text.strip().split('\n')

    # Parse the classification into a dictionary
    result = {}
    current_category = None
    for line in classification:
        if line.endswith(':'):
            current_category = line[:-1].strip()
            result[current_category] = []
        elif current_category:
            result[current_category].append(line.strip())

    return result

field_names = [
    'Descripción del Problema',
    'Beneficios',
    'Título de tu Idea',
    'Propuesta',
    'Nivel de Retorno Esperado (En Pesos)',
    '¿A qué tipo de clientes u organizaciones podría llegar esta idea/solución?',
    'Otros Comentarios',
    'Autor (es) de la idea, número de identificación y proyecto o área a la que pertenecen.',
    'Rut',
    'Participantes',
    '¿Qué nueva propuesta de valor, modelo de negocios o producto podría llevar SRE al mercado? / What new value proposition, business model or product could SRE bring to the market?',
    'Beneficios para el cliente y el Banco',
    'Lema/Slogan',
    'Nombre Iniciativa',
    'Propuesta de Idea / Solución',
    '¿ Cómo responde esta idea al desafío?',
    '¿Para implementar esta idea, cuáles son los siguientes pasos?',
    'Alcance e impacto del Proyecto (máximo ½ página)',
    'Impacto',
    'Nombre de la solución',
    'Descripción',
    'Descripción del proyecto (máximo 2 páginas)',
    'Inicio',
    'Descripción de la Propuesta',
    '% de alineamiento con el foco estratégico',
    'Nombre ',
    'Mecanismos de transferencia (máximo 1 página)',
    'Nombre Completo',
    'Teléfono',
    'Término',
    '¿Con qué activos cuenta SRE que podrían facilitar la implementación de este nuevo servicio, modelo de negocios o producto? / What assets does SRE have that could facilitate the implementation of this new service, business model or product?',
    'Para implementar tu propuesta, ¿Se necesitan captar nuevos datos que hoy no están disponibles?',
    '¿Hoy se toman decisiones en torno a la pregunta que identificaste?',
    'Alcance',
    'Preguntas guías',
    '¿Por qué? ',
    'Propuesta de valor del proyecto',
    '¿Qué es lo que busca tu idea?',
    '¿En que planta tiene origen tu idea?',
    'Idea / Solución Propuesta',
    'Nombre de Actividad'
]

classification = classify_field_names(field_names)
print(classification)
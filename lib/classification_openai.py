import openai
from dotenv import load_dotenv
import os
import pandas as pd
import time
from openai.error import APIError

load_dotenv()
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
OPENAI_ORG_ID = os.environ.get('OPENAI_ORG_ID')
openai.api_key = OPENAI_API_KEY
openai.organization = OPENAI_ORG_ID

forms_by_company = pd.read_json(r'H:\Mi unidad\Innk\fields_by_company.json')
titles = forms_by_company['field_title'].apply().to_list()
descriptions = forms_by_company['field_description'].replace('','No description').to_list()
companies_id = forms_by_company['company_id'].to_list()


def classify_field(title:str, description:str):
    """ Function to classify a field into one of the following categories: 
    Problem, Solution or Idea name, Other. Usinf OpenAI's API.

    Args:
        title (str): field name
        description (str): field description

    Returns:
        response: final classification
    """
    
    message = f"I have a form field with the title: '{title}' \
            and the description: '{description}'. Classify the title into the categories: Problem, Solution, \
                Idea name, Other. Using the title and its description. \
                \n\
                Here are some examples for correct classification:\
                Example 1:\
                Title = '¿Cuál es la oportunidad o el problema sobre el que surge tu idea?'\
                Description = '¿Cuál es la oportunidad o el problema sobre el que surge tu idea? \
                    / In this section you should describe the specific problem you want to solve or \
                        opportunity you want to take advantage of.'\
                Category = 'Problem'\
                Example 2:\
                Title = '¿Qué es lo que busca tu idea?'\
                Description = 'Selecciona que acción apunta a lograr tu idea.'\
                Category = 'Solution'\
                Example 3:\
                Title: 'Tu propuesta de nombre'\
                Description: 'Resume en un par de palabras la acción que busca generar tu idea.\
                    Un buen titulo no debería tener más de 5 palabras.'\
                Category: 'Idea name'\
                Example 4:\
                Title: '¿Cuál es el tiempo estimado para desarrollarla?'\
                Description: 'Si no sabes exactamente puedes poner referencias o escribir \
                    Sin Información. Recuerda que mientras más antecedentes tengamos, podremos evaluar\
                        tu idea de mejor manera.'\
                Category: 'Other'\
                \n\
                Here are some examples of incorrect classification:\
                Example 1:\
                Title = '¿Cómo crees que podrían ser solucionados los problemas abordados para asegurar que tu equipo “vuele”?'\
                Description = 'En esta sección debes describir cuál es tu propuesta para el problema u oportunidad detectados.\
                    Sintetiza como funcionaria la solución que propones y en qué se diferencia de otras formas de abordar el \
                        problema.'\
                Category = 'Problem'\
                Example 2:\
                Title = '¿Qué es lo que busca tu idea?'\
                Description = 'Selecciona que acción apunta a lograr tu idea.'\
                Category =  'Solution'\
                Example 3:\
                Title = 'La necesidad'\
                Description = '¿A qué problema, necesidad, falla o quiebre del mercado se enfoca su proyecto?'\
                Category = 'Idea name'\
                Example 4:\
                Title = 'Contexto'\
                Description = 'Cuéntanos detalles sobre la necesidad, oportunidad o situación'\
                Category = 'Other'\
                \n\
                Give only the category as an answer."
    response = openai.ChatCompletion.create(
            model ="gpt-3.5-turbo",
            messages = [{"role": "system", 
                          "content": "You are a helpful assistant."}, 
                         {"role": "user", "content": message}],
        max_tokens=100,
        temperature=0,
    )
    return response.choices[0].message['content']


def classify_fields_complete(titles:list, descriptions:list, companies_id:list, start_index=0)->dict:
    """Function that iterates through a list of titles and descriptions and classifies them using the 
    classify_field function. 

    Args:
        titles (list): list of titles
        descriptions (list): list of descriptions
        companies_id (list): list of companies id
        start_index (int, optional): dictates from wich index of the list should start the iteration, incase an error
        occurs, the function will return the dictionary till the point of error and the index in wich the error occurs
        so function may restart from that index. Defaults to 0.

    Returns:
        dict: dictionary with the classification of the fields, the description and the company id
    """

    classification_list = []
    n = start_index
    for title, description, comp_id in zip(titles[start_index:], descriptions[start_index:], companies_id[start_index:]):
        try:
            if n ==1165:
                return classification_list
            print(n)
            d = {}
            classification = classify_field(title, description)
            d[n] = {title: classification,
                    'description':description,
                    'company_id':comp_id}
            n += 1
            classification_list.append(d)
            time.sleep(2)
        except Exception as e:
            try:
                time.sleep(5)
                classification = classify_field(title, description)
                d[n] = {title: classification,
                        'description':description,
                        'company_id':comp_id}
                n += 1
                classification_list.append(d)
                time.sleep(2)
                
            except Exception as e:
                print(e)
                print('Error occurs at index: ' + str(n))
                return classification_list

    return classification_list

# Usage:


def main():
    classification = classify_fields_complete(titles, descriptions, companies_id, 0)

    class_list = []
    for i in classification:
        if type(i) is dict:
            class_list.append(i)
        elif type(i) is list:
            for j in i:
                if type(j) is dict:
                    class_list.append(j)


    new_list = []
    # Process each dictionary
    for dictionary in class_list:
        # Get the first dictionary inside the outer dictionary
        inner_dict = list(dictionary.values())[0]
        # Get the first key-value pair in the inner dictionary (ignoring 'description' and 'company_id')
        field, category = next((k, v) for k, v in inner_dict.items() if k not in ['description', 'company_id'])
        # Create a new dictionary with the desired structure and add it to the list
        new_list.append({
            'company_id': inner_dict['company_id'],
            'description': inner_dict['description'],
            'field': field,
            'category': category
        })

    # Create a DataFrame from the list
    df = pd.DataFrame(new_list)
    
    return df

if __name__ == "__main__":
    main()
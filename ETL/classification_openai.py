import openai
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
OPENAI_ORG_ID = os.environ.get('OPENAI_ORG_ID')
openai.api_key = OPENAI_API_KEY
openai.organization = OPENAI_ORG_ID

forms_by_company = pd.read_json(r'H:\Mi unidad\Innk\fields_by_company.json')
titles = forms_by_company['field_title'].apply().to_list()
descriptions = forms_by_company['field_description'].replace('','No description').to_list()
companies_id = forms_by_company['company_id'].to_list()


def classify_field(title, description):
    """ Function to classify a field into one of the following categories: 
    Problem, Solution or Idea name, Other.

    Args:
        title (str): field name
        description (str): field description

    Returns:
        response: final classification
    """
    
    message = f"I have a form field with the title '{title}' \
            and the description '{description}'. Classify the title into the categories: Problem, Solution or \
                Idea name, Other. Give only the category as an answer."
    response = openai.ChatCompletion.create(
            model ="gpt-3.5-turbo",
            messages = [{"role": "system", 
                          "content": "You are a helpful assistant."}, 
                         {"role": "user", "content": message}],
        max_tokens=100,
        temperature=0.3,
    )
    return response.choices[0].message['content']


def classify_fields(titles, descriptions, companies_id):
    classification_dict = {}
    for title, description, comp_id in zip(titles, descriptions, companies_id):
        try:
            classification = classify_field(title, description)
            classification_dict[title] = classification
            classification_dict['company_id'] = comp_id
        except Exception as e:
            print(e)
        finally:
            print('Error occurs in index: ' + str(titles.index(title)))
            return classification_dict

    return classification_dict

# Usage:

classification_dict = classify_fields(titles, descriptions, companies_id)

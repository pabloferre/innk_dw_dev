import pandas as pd
import numpy as np
import os
import sys
from sklearn.cluster import KMeans
import sys
import boto3
from datetime import datetime
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from app.cluster_utils import get_conn, execute_sql, average_valid_vectors, deserialize_vector
from dotenv import load_dotenv

load_dotenv()
bucket_name = os.environ.get('bucket_name')
aws_access_id = os.environ.get('aws_access_id')
aws_access_key = os.environ.get('aws_access_key')
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')

today = datetime.today().strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

#################################################CLASSES ####################################################



class ClusterIdeas():
    
    def __init__(self, data, n_clusters=5, n_init=50, max_iter=500, init='k-means++'):
        self.data = data
        self.n_clusters = n_clusters
        self.max_iter = max_iter
        self.labels = ''
        self.max_clusters = 25
        self.n_init = n_init
        self.init = init
        self.kmeans = KMeans(n_clusters=self.n_clusters, init=self.init, n_init=self.n_init, max_iter=self.max_iter ,random_state=42)
        
    
    def fit(self):
        self.kmeans.fit(self.data)
        self.labels = self.kmeans.labels_
        self.centroids = self.kmeans.cluster_centers_
        return 
    
    def distance_to_centroid(self):
        labels = self.labels

        # Obtain the centroids
        centroids = self.centroids

        # Compute distances
        distances = []
        for i, label in enumerate(labels):
            centroid = centroids[label]
            distance = np.linalg.norm(self.data[i] - centroid)
            distances.append(distance)

        return distances
        


#################################################AUXILARY FUNCTIONS####################################################

def cluster_group(group):
    '''Function to perform clustering on a group of rows and return the group with cluster info'''
    n_clusters = round(len(group['combined_emb'])/10)
    # Ensure there is at least one cluster
    n_clusters = max(n_clusters, 1)
    
    cideas = ClusterIdeas(np.array(group['combined_emb'].tolist()), n_clusters=n_clusters, max_iter=500)
    cideas.fit()
    group['distance_centroid'] = cideas.distance_to_centroid()
    group['cluster_number'] = cideas.labels
    return group

#################################################MAIN FUNCTION####################################################


def main():
    
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_id,
                            aws_secret_access_key=aws_access_key)

    
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    query = """Select dim_idea.id, idea_db_id, name, description, solution_1, name_embedded, prob_1_embedded, sol_1_embedded, prob_2_embedded, sol_2_embedded,
        prob_3_embedded, sol_3_embedded, prob_4_embedded, sol_4_embedded, prob_5_embedded, sol_5_embedded,
            prob_6_embedded, sol_6_embedded, f.goal_id, f.company_id 
        from public.dim_idea
        left join public.fact_submitted_idea as f on f.idea_id = dim_idea.id"""

    embedded_columns = ['name_embedded', 'prob_1_embedded', 'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded', 
                    'prob_3_embedded', 'sol_3_embedded', 'prob_4_embedded', 'sol_4_embedded', 'prob_5_embedded', 
                    'sol_5_embedded', 'prob_6_embedded', 'sol_6_embedded']

    df = pd.DataFrame(execute_sql(query, conn), columns=['id', 'idea_db_id', 'name', 'description', 'solution_1', 'name_embedded', 'prob_1_embedded',
        'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded', 'prob_3_embedded', 'sol_3_embedded', 'prob_4_embedded',
        'sol_4_embedded', 'prob_5_embedded', 'sol_5_embedded', 'prob_6_embedded', 'sol_6_embedded', 'f.goal_id', 'f.company_id'])
    df.rename(columns={'f.goal_id':'goal_id', 'f.company_id':'company_id'}, inplace=True)
    
    query2 = """Select id, idea_id, idea_db_id from public.param_clustered_ideas"""
    df_clustered = pd.DataFrame(execute_sql(query2, conn), columns=['id', 'idea_id', 'idea_db_id'])
    
    df = df.loc[~df.loc[:,'idea_db_id'].isin(df_clustered.loc[:,'idea_db_id'])].reset_index(drop=True)
    
    conn.close()
    
    for col in embedded_columns:
        df[col] = df[col].apply(lambda x: deserialize_vector(x))

    df['combined_emb'] = df.apply(lambda row: average_valid_vectors(row, embedded_columns), axis=1)

    df = df.loc[df.loc[:,'combined_emb'].notnull()].reset_index(drop=True)

    df['distance_centroid'] = None
    df['cluster_number'] = None

    df = df.groupby('company_id').apply(cluster_group)

    df.reset_index(drop=True, inplace=True)

    # Keep only the relevant columns
    df = df[['idea_db_id', 'company_id', 'goal_id', 'cluster_number', 'distance_centroid', 'name', 'description', 'solution_1']]

    # Define the bucket name and S3 file name

    s3_file_name = 'raw/' + str(today) + '_clustered_ideas.json'

    # Upload the JSON string to S3
    json_file = df.to_json(orient='records')
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=json_file)

    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name}'


    print(url)
    
    sys.stdout.write(url)
    
    return url

if __name__ == '__main__':
    main()

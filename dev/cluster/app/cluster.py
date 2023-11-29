import pandas as pd
import numpy as np
import os
import joblib
import sys
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
import sys
path = os.path.dirname(os.path.realpath(__file__))
os.chdir(path)
from APP_module import get_conn, execute_sql, average_valid_vectors, deserialize_vector
from dotenv import load_dotenv

load_dotenv()
path_to_drive = os.environ.get('path_to_drive')
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')



class ClusterIdeas():
    
    def __init__(self, data, n_clusters=5, n_init=50, max_iter=500, init='k-means++'):
        self.data = data
        self.n_clusters = n_clusters
        self.max_iter = max_iter
        self.labels = ''
        self.max_clusters = 25
        self.n_init = n_init
        self.init = init
        self.kmeans = KMeans(n_clusters=self.n_clusters, init= self.init, n_init=self.n_init, max_iter=self.max_iter ,random_state=42)
        
    
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
        
    def silhouette_score(self):
        sil = []

        for i in range(2, self.max_clusters + 1):  # Silhouette score requires at least 2 clusters.
            kmeans_s = KMeans(n_clusters=i, init='k-means++', random_state=42)
            kmeans_s.fit(self.data)
            silhouette_avg = silhouette_score(self.data, kmeans_s.labels_)
            sil.append(silhouette_avg)

        plt.plot(range(2, self.max_clusters + 1), sil)
        plt.title('Silhouette Analysis')
        plt.xlabel('Number of clusters')
        plt.ylabel('Silhouette Score')
        plt.show()

    def tnse_visualization(self):
        tsne = TSNE(n_components=2, random_state=42)
        tsne_results = tsne.fit_transform(self.data)

        plt.scatter(tsne_results[:, 0], tsne_results[:, 1], c=self.kmeans.labels_, cmap='rainbow')
        plt.title('t-SNE Visualization of Clusters')
        plt.show()
    
    def pca_visualization(self):
        pca = PCA(n_components=2)
        pca_results = pca.fit_transform(self.data)

        plt.scatter(pca_results[:, 0], pca_results[:, 1], c=self.kmeans.labels_, cmap='rainbow')
        plt.title('PCA Visualization of Clusters')
        plt.show()
        
        
conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = """Select dim_idea.id, idea_db_id, name_embedded, prob_1_embedded, sol_1_embedded, prob_2_embedded, sol_2_embedded,
    prob_3_embedded, sol_3_embedded, prob_4_embedded, sol_4_embedded, prob_5_embedded, sol_5_embedded,
        prob_6_embedded, sol_6_embedded, f.goal_id, f.company_id 
    from innk_dw_dev.public.dim_idea
    left join innk_dw_dev.public.fact_submitted_idea as f on f.idea_id = dim_idea.id"""

embedded_columns = ['name_embedded', 'prob_1_embedded', 'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded', 
                'prob_3_embedded', 'sol_3_embedded', 'prob_4_embedded', 'sol_4_embedded', 'prob_5_embedded', 
                'sol_5_embedded', 'prob_6_embedded', 'sol_6_embedded']

df = pd.DataFrame(execute_sql(query, conn), columns=['id', 'idea_db_id', 'name_embedded', 'prob_1_embedded',
    'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded', 'prob_3_embedded', 'sol_3_embedded', 'prob_4_embedded',
    'sol_4_embedded', 'prob_5_embedded', 'sol_5_embedded', 'prob_6_embedded', 'sol_6_embedded', 'f.goal_id', 'f.company_id'])
df.rename(columns={'f.goal_id':'goal_id', 'f.company_id':'company_id'}, inplace=True)
conn.close()

#df_ = pd.read_parquet(path_to_drive + r'stage/dim_idea.parquet')
df = df.loc[df['company_id'] == 94].reset_index(drop=True) #ejemplo para Dev, eliminar para prod

for col in embedded_columns:
    df[col] = df[col].apply(lambda x: deserialize_vector(x))
    
df['combined_emb'] = df.apply(lambda row: average_valid_vectors(row, embedded_columns), axis=1)
df = df.loc[df.loc[:,'combined_emb'].notnull()].reset_index(drop=True)
n_clusters = round(len(df.loc[:,'combined_emb'])/10)


cideas = ClusterIdeas(np.array(df.loc[:,'combined_emb'].tolist()), n_clusters=n_clusters, max_iter=300)
cideas.fit()
df['distance_centroid'] = cideas.distance_to_centroid()
df['cluster_number'] = cideas.labels
df = df[['idea_db_id', 'company_id', 'goal_id', 'cluster_number', 'distance_centroid']]

        
        
        
conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
for i, row in df.iterrows():
    print(i)
    query = f''' UPDATE public.param_clustered_ideas
        SET distance_centroid = {row['distance_centroid']}
        WHERE idea_db_id = {row['idea_db_id']};'''
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
conn.close()

clusters = ClusterIdeas(df['combined_emb'].values.tolist(), n_clusters=136, max_iter=500)
clusters.fit()
joblib.dump(clusters.kmeans, path_to_drive + r'models/clusters_117.pkl')

df['labels'] = clusters.labels
df['labels_predict'] = ''
model = joblib.load(path_to_drive + r'models/clusters_117.pkl')

df.loc[1,'lables_predict'] = model.predict(df.loc[1,'combined_emb'].reshape(1, -1))
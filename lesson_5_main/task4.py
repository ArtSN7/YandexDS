import pandas as pd
from sklearn.cluster import KMeans


def process(df):
    features = [col for col in df.columns if col not in ['ID', 'Best Position']]

    X = df[features].values

    kmeans = KMeans(n_clusters=10, random_state=123)
    cluster_labels = kmeans.fit_predict(X)

    df['cluster'] = cluster_labels
    cluster_counts = df['cluster'].value_counts()
    largest_cluster_id = cluster_counts.idxmax()  #

    largest_cluster_data = df[df['cluster'] == largest_cluster_id]
    most_common_position = largest_cluster_data['Best Position'].mode()[0]

    return most_common_position


if __name__ == "__main__":
    df = pd.read_csv('Football Players Clustering.csv')
    print(process(df))

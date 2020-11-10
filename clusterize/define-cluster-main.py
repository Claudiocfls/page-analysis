
from Database import DatabaseService
from regex_clusters_by_url import find_cluster_by_regex_for_site_example
import re

def get_clustering_strategy(label):
    strategies = {
        'pages_label_1': find_cluster_by_regex_for_site_example,
        # ...all other labels
    }
    def default_strategy(url):
        return 0

    strategy = strategies.get(label)
    return strategy if strategy else default_strategy

def clusterize_crawler_results(db, label):
    rows = db.get_id_and_url_from_specific_label(label)
    find_cluster_by_regex = get_clustering_strategy(label)
    for row in rows:
        id, url = row
        try:
            cluster = find_cluster_by_regex(url)
            db.insert_cluster(id, cluster)
        except Exception as error:
            print("(crawler_results) Failure on id {}".format(id))
            print(error)

def clusterize_target_links(db, label):
    find_cluster_by_regex = get_clustering_strategy(label)
    for row in db.target_link_generator(label):
        id, url = row
        try:
            cluster = find_cluster_by_regex(url)
            db.insert_cluster_on_hyperlinks_pairs(id, cluster)
        except Exception as error:
            print("(target) Failure on id {}".format(id))
            print(error)

if __name__ == '__main__':
    db = DatabaseService.get_instance()
    label = 'pages_label_1'
    clusterize_crawler_results(db, label)
    clusterize_target_links(db, label)
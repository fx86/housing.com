from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import pandas as pd
import numpy as np

N = 40

df = pd.read_csv("housing.csv", encoding="utf-8")
df = df.drop_duplicates('id')
columns = {"completion_date": "completed_on", "ctr": "CTR", 
            "date_added": "date_added", 
            "date_added_in_seconds": "listing_date_seconds", 
            "developer_name": "developer_name", 
            "featured_type": "featured_type", 
            "impressions": "impressions", 
            "inventory_amenities_has_gas_pipeline": "has_gas", 
            "inventory_amenities_has_gym": "has_gym", 
            "inventory_amenities_has_lift": "has_lift",
            "inventory_amenities_has_parking": "has_parking", 
            "inventory_amenities_has_servant_room": "has_servant_room",
            "inventory_amenities_has_swimming_pool": "has_pool", 
            "inventory_amenities_is_gated_community": "has_gated_community",
            "inventory_canonical_url": "URL",
            "inventory_configs_0_apartment_type_id": "appt_type_id",
            "inventory_configs_0_area": "area",
            "inventory_configs_0_facing": "facing",
            "inventory_configs_0_is_available": "is_available",
           "inventory_configs_0_number_of_bedrooms": "bedrooms",
           "inventory_configs_0_number_of_toilets": "toilets",
           "inventory_configs_0_parking_count": "parking_count",
           "inventory_configs_0_per_sqft_rate": "sqft_rate",
           "inventory_configs_0_price": "price",
           "inventory_configs_0_property_type_id": "property_type",
           "is_blocked": "is_blocked", "is_featured": "is_featured",
           "is_uc_property": "is_uc_property", "polygons_hash_city_name": "city",
           "polygons_hash_housing_region_name": "housing_region",
           "polygons_hash_locality_name": "locality",
           "polygons_hash_state_name": "state",
           "polygons_hash_sublocality_name": "sub_locality",
           "price_on_request": "price_on_request", "show_loan_option": "has_loan",
           "status": "status", "street_info": "street", "title": "title",
           "type": "sale_type", "updated_at": "updated_on", "_id": "id"}

df = df[columns.keys()]
df = df.rename(columns=columns)

timestamp_columns = 'completed_on updated_on'.split(' ')
for col in timestamp_columns:
    df[col] = df[col].apply(lambda v: pd.to_datetime(v, unit='s'))

df['date_added'] = pd.to_datetime(df['date_added'])
df['Age_of_property'] = (pd.datetime.now() - df['date_added']).dt.days

df['Independent_floor'] = df.title.apply(
    lambda v: 1 if 'Independent Floor' in v else 0)
df['Independent_house'] = df.title.apply(
    lambda v: 1 if 'Independent House' in v else 0)

kmeans_input = df.copy()

# filter out columns with boolean column names
boolean_columns = kmeans_input.select_dtypes(include=['bool']).columns

# convert boolean columns into integer formatted 0s and 1s
kmeans_input[boolean_columns] = kmeans_input[boolean_columns].astype(int)

# select only integer and float columns
kmeans_input = kmeans_input.select_dtypes(
    include=['int32', 'int64', 'float64'])

# remove ones like dates-in-seconds
for col in ['listing_date_seconds', 'property_type', 'impressions']:
    print col, col in kmeans_input
    if col in kmeans_input:
        kmeans_input = kmeans_input.drop(col, 1)


def normalize_values(series):
    #     series = (series - series.mean())/series.std()
    series = (series - series.min()) / (series.max() - series.min())
    return series


def optimalK(data, nrefs=3, maxClusters=15):
    """
    Calculates KMeans optimal K using Gap Statistic from Tibshirani, Walther, Hastie
    Params:
        data: ndarry of shape (n_samples, n_features)
        nrefs: number of sample reference datasets to create
        maxClusters: Maximum number of clusters to test for
    Returns: (gaps, optimalK)
    """
    gaps = np.zeros((len(range(1, maxClusters)),))
    resultsdf = pd.DataFrame({'clusterCount': [], 'gap': []})
    for gap_index, k in enumerate(range(1, maxClusters)):

        # Holder for reference dispersion results
        refDisps = np.zeros(nrefs)

        # For n references, generate random sample and perform kmeans getting
        # resulting dispersion of each loop
        for i in range(nrefs):

            # Create new random reference set
            randomReference = np.random.random_sample(size=data.shape)

            # Fit to it
            km = KMeans(k)
            km.fit(randomReference)

            refDisp = km.inertia_
            refDisps[i] = refDisp

        # Fit cluster to original data and create dispersion
        km = KMeans(k)
        km.fit(data)

        origDisp = km.inertia_

        # Calculate gap statistic
        gap = np.log(np.mean(refDisps)) - np.log(origDisp)

        # Assign this loop's gap statistic to gaps
        gaps[gap_index] = gap

        resultsdf = resultsdf.append(
            {'cluster_count': k, 'gap': gap}, ignore_index=True)

    # Plus 1 because index of 0 means 1 cluster is optimal, index
    return (gaps.argmax() + 1, resultsdf)

for col in ['area', 'price', 'sqft_rate', 'Age_of_property']:
    if col in kmeans_input:
        kmeans_input[col] = normalize_values(kmeans_input[col])

df = pd.DataFrame({'cluster_count': [], 'avg_silh_score': [], 'inertia': []})
N_max = 400
for i in range(7, 8):
    print "cluster ", i
    kmeans = KMeans(n_clusters=i, random_state=0).fit(kmeans_input)
    df = df.append({'cluster_count': i, 'avg_silh_score': silhouette_score(
        kmeans_input, kmeans.labels_), 'inertia': kmeans.inertia_}, ignore_index=True)

df.to_csv("kmeans_score.csv", index=False)
gap_stat, optimalK_output = optimalK(kmeans, maxClusters=N_max)
print "gap statistic : ", gap_stat
optimalK_output.to_csv("optimalK.csv", index=False)

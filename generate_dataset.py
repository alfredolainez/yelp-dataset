"""
How to generate dataset

Probably won't work with less than 16GB RAM
"""

from data_handling import pickles_from_json, get_reviews_data, get_business_data, \
    give_balanced_classes, create_data_sets

NUM_PARTITIONS = 100

# First generate the pickles with all the data in python format
# Although this could be ignored and directly generate the relevant data,
# having the files can be nice for future expansion of the data
pickles_from_json(NUM_PARTITIONS)

# We need business data to filter reviews from outside of the US (only English)
business_data = get_business_data()

# Get data from partitions created, partition by partition
review_texts = []
useful_votes = []
funny_votes = []
cool_votes = []
review_stars = []
for partition in range(1, NUM_PARTITIONS + 1):
    data = get_reviews_data((partition, ), business_data, not_include_states=["EDH", "QC", "BW"])
    (texts, useful, funny, cool, stars) = data
    review_texts.extend(texts)
    useful_votes.extend(useful)
    funny_votes.extend(funny)
    cool_votes.extend(cool)
    review_stars.extend(stars)

# Generate dataset funny reviews
reviews, labels = give_balanced_classes(review_texts, funny_votes, votes_threshold=3)
result = create_data_sets(reviews, labels, write_to_pickle=True, problem="funny")
(train_reviews, train_labels, dev_reviews, dev_labels, test_reviews, test_labels) = result

# Generate dataset of useful reviews
reviews, labels = give_balanced_classes(review_texts, useful_votes, votes_threshold=3)
result = create_data_sets(reviews, labels, write_to_pickle=True, problem="useful")

# Generate dataset of cool reviews
reviews, labels = give_balanced_classes(review_texts, cool_votes, votes_threshold=3)
result = create_data_sets(reviews, labels, write_to_pickle=True, problem="cool")
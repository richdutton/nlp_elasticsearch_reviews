# coding: utf-8
# # ElasticSearch Process and Tips (Indexing, Analyzing, Querying)

# ### Things About Your Local ES Installation
#
# There is a config directory where you have ES installed.  This is where you would edit the config yml file, or add files for stopwords or synonyms.  (This is important information you will need, so make sure you know where your install lives.)
#

# To start up: Run elasticsearch (bin/elasticsearch), then run bin/kibana inside the kibana directory.  This establishes the plugins you might want, like Sense.  See below re Sense.

# ### The Sense Plugin to Kibana and ES is Useful:

# Install Kibana, and then add plugins (they become "apps" under the tab in the Kibana toolbar. (The little grid icon to the right of the "Sense" name in the pic below.)
#
# Sense plugin: https://github.com/bleskes/sense  (Note, it used to be part of Marvel.)
# Install directions: https://www.elastic.co/guide/en/sense/current/installing.html
#
# **Example: in the ES docs**:
#
#     curl 'localhost:9200/_cat/indices?v'
#
# **In Sense**:
#
#     GET /_cat/indices?v
#
# <img src="images/sense_ui.png">
#
# In the github repo, there is a text file of Sense queries that will work after you index your data using the steps below.

# ### Using Python...  Slightly different API:
#
# We will be loading the data from the stored dataframe, and then indexing the data in ES.  We can do queries against it from Python or using the Sense plugin.

from __future__ import print_function
from pprint import pprint
import json

from elasticsearch import Elasticsearch, client
import pandas as pd


def main():
    # es = Elasticsearch(hosts=[{'host': 'elasticsearch.aws.blahblah.com', 'port': '9200'}])
    local_es = Elasticsearch()
    local_client = client.IndicesClient(local_es)

    # ### Analyzers, Defaults, and Preventing Analysis
    #
    # Analysis is the process of chopping up your text and storing it in a form that can be searched efficiently against.
    #
    # #### Read this:
    #
    # https://www.elastic.co/guide/en/elasticsearch/guide/current/custom-analyzers.html
    #
    # An Analyzer, is in order, a sequence of optional
    # * character filters
    # * tokenizers
    # * token filters
    #
    # To prevent analysis, you can specify "not_analyzed" on the index itself.  The Interwebs also suggest "keyword" as the analyzer for a field, but some folks claim it does some simple analyis.
    #
    # The default analyzer (if unspecified!) for string fields is "standard."  In a custom analyzer, it would be defined:
    #
    #     {
    #         "type":      "custom",
    #         "tokenizer": "standard",
    #         "filter":  [ "lowercase", "stop" ]
    #     }
    #
    # More on default analysis from the docs (https://www.elastic.co/guide/en/elasticsearch/guide/current/_controlling_analysis.html):
    #
    # >While we can specify an analyzer at the field level, how do we determine which analyzer is used for a field if none is specified at the field level?
    # >
    # >Analyzers can be specified at several levels. Elasticsearch works through each level until it finds an analyzer that it can use. At index time, the order is as follows:
    # >
    # >1. The analyzer defined in the field mapping, else
    # >2. The analyzer named default in the index settings, which defaults to
    # >3. The standard analyzer
    # >
    # >...At search time, the sequence is slightly different:...
    # >
    # >1. The analyzer defined in the query itself, else
    # >2. The search_analyzer defined in the field mapping, else
    # >3. The analyzer defined in the field mapping, else
    # >4. The analyzer named default_search in the index settings, which defaults to
    # >5. The analyzer named default in the index settings, which defaults to
    # >6. The standard analyzer
    #
    # #### We can inspect analysis with the "analyze" function (or "_analyze" in the curl style).
    if local_es.indices.exists('my_index'):
        local_es.indices.delete(index='my_index')
    local_es.indices.create(index='my_index')

    # this is the default analyzer ES will use if you don't specify one! Specify one!
    print(local_client.analyze(index='my_index', analyzer='standard', text='My kitty-cat is adorable.'))

    # A utility to make analysis results easier to read:
    def get_analyzer_tokens(result):
        ''' Utility to combine tokens in an analyzer result. '''
        tokens = result[u'tokens']
        print(tokens)
        return ' '.join([token['token'] for token in tokens])

    get_analyzer_tokens(local_client.analyze(index='my_index', analyzer="standard", text='My kitty-cat\'s a pain in the neck.'))

    # **NB: Prevent analysis with "keyword" analyzer, or set the index itself as "not_analyzed" in settings.**
    #
    # But if you do this, you need to match on EXACT field contents to search for it.  Best to keep an analyzed copy too, if it's meant to be english searchable text.
    get_analyzer_tokens(local_client.analyze(index='my_index', analyzer='keyword', text='My kitty-cat\'s a pain in the neck.'))

    # ## The Built-In ES "English" Analyzer:
    # ### A useful analyzer for text is the built-in English one, which does this, approximately:
    #
    # https://www.elastic.co/guide/en/elasticsearch/guide/current/language-intro.html
    #
    # See:
    # https://simpsora.wordpress.com/2014/05/02/customizing-elasticsearch-english-analyzer/
    #
    # >Tokenizer: Standard tokenizer
    #
    # >TokenFilters:
    # >* Standard token filter
    # >* English possessive filter, which removes trailing 's from words
    # >* Lowercase token filter
    # >* Stop token filter
    # >* Keyword marker filter, which protects certain tokens from modification by stemmers
    # >* Porter stemmer filter, which reduces words down to a base form (“stem”)
    #
    #
    # These are the stop-words defined for English:
    #
    #     a, an, and, are, as, at, be, but, by, for, if, in, into, is, it,
    #     no, not, of, on, or, such, that, the, their, then, there, these,
    #     they, this, to, was, will, with
    #
    # If you want to customize you can create a new filter yourself or use a file in your config directory for ES.
    # Try it on some text and see...
    get_analyzer_tokens(local_client.analyze(index='my_index', analyzer='english', text='My kitty-cat\'s a pain in the neck.'))

    # If you wanted to customize the 'english' analyzer with your own special rules (extra stopwords etc), see here: https://www.elastic.co/guide/en/elasticsearch/guide/current/configuring-language-analyzers.html
    #

    # ## Analyzers and Custom Analyzers

    # You want to make sure you are explicit about types in your data, so that ES doesn't just guess and maybe get it wrong. Also, this is how you set explicit analysis.

    #
    #
    # Create a setting for the index:
    #
    #     PUT /my_index
    #     {
    #         "settings": {
    #             "analysis": {
    #                 "char_filter": { ... custom character filters ... },
    #                 "tokenizer":   { ...    custom tokenizers     ... },
    #                 "filter":      { ...   custom token filters   ... },
    #                 "analyzer":    { ...    custom analyzers referring to the definitions above ... }
    #             }
    #         }
    #     }
    #
    # For example - this saves a bunch of analysis components into an analyzer called 'my_analyzer':
    #
    #     PUT /my_index
    #     {
    #         "settings": {
    #             "analysis": {
    #                 "char_filter": {
    #                     "&_to_and": {
    #                         "type":       "mapping",
    #                         "mappings": [ "&=> and "]
    #                 }},
    #                 "filter": {
    #                     "my_stopwords": {
    #                         "type":       "stop",
    #                         "stopwords": [ "the", "a" ]
    #                 }},
    #                 "analyzer": {
    #                     "my_analyzer": {
    #                         "type":         "custom",
    #                         "char_filter":  [ "html_strip", "&_to_and" ],
    #                         "tokenizer":    "standard",
    #                         "filter":       [ "lowercase", "my_stopwords" ]
    #                 }}
    #     }}}
    #
    #  Then you **use it**, by referring to it in a mapping for a document in this index:
    #
    #      PUT /my_index/_mapping/my_type
    #     {
    #         "properties": {
    #             "title": {
    #                 "type":      "string",
    #                 "analyzer":  "my_analyzer"
    #             }
    #         }
    #     }
    #
    # #### Remember: If you don't assign it to a field in a mapping, you aren't using it.
    #
    # In Python:

    MY_SETTINGS = {
        "settings": {
            "analysis": {
                "char_filter": {
                    "&_to_and": {
                        "type": "mapping",
                        "mappings": ["&=> and "]}},
                "filter": {
                    "my_stopwords": {
                        "type": "stop",
                        "stopwords": ["the", "a"]}},
                "analyzer": {
                    "my_analyzer": {
                        "type": "custom",
                        "char_filter": ["html_strip", "&_to_and"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "my_stopwords"]}}
            }}
    }

    MAPPING = {
        "my_doc_type": {
            "properties": {
                "title": {
                    "type": "string",
                    "analyzer": "my_analyzer"
                }
            }
        }
    }

    # ## Stopwords Note
    #
    # The default list of stopwords is indicated thusly:
    #
    # >"stopwords": "\_english\_"
    #
    # So you can specify both that filter and a custom stopwords list, if you want.
    if local_es.indices.exists('my_index'):
        local_es.indices.delete(index='my_index')
    local_es.indices.create(index='my_index', body=json.dumps(MY_SETTINGS))
    local_es.indices.put_mapping(index='my_index', doc_type="my_doc_type", body=json.dumps(MAPPING))

    # Check that your mapping looks right!
    print(local_client.get_mapping(index='my_index'))

    res = local_client.analyze(index='my_index', analyzer='my_analyzer', text="<p>This is the title & a Capitalized Word!</p>")
    get_analyzer_tokens(res)

    # ## Tokenizers vs. Analyzers - Be Careful.
    #
    # Some of the names in ES are confusing.  There is a **"standard" analyzer** and a **"standard" tokenizer**. https://www.elastic.co/guide/en/elasticsearch/guide/current/standard-tokenizer.html#standard-tokenizer
    #
    # Check them out:
    get_analyzer_tokens(local_client.analyze(index='my_index', analyzer='standard', text='My kitty-cat\'s not a pain in the \'neck\'!'))

    #  The difference is subtle but there.
    get_analyzer_tokens(local_client.analyze(index='my_index', tokenizer="standard", text='My kitty-cat\'s not a pain in the \'neck\'!'))

    # However, if you use the english analyzer it will override that uppercase and also remove the negation,
    # because "not" is in the stopwords list:
    get_analyzer_tokens(local_client.analyze(index='my_index', analyzer="english", tokenizer="standard",
                                             text='My kitty-cat\'s not a pain in the \'neck\'!'))

    # ## Indexing Yelp Data
    df = pd.read_msgpack("./data/yelp_df_forES.msg")
    print(df.head())

    # test with a small sample if you want
    dfshort = df.query('stars >= 5 and net_sentiment > 35')
    print(len(dfshort))
    print(dfshort.head())

    # filter out any rows with a nan for sent_per_token, which breaks bulk load:
    df = df[df.sent_per_token.isnull() != True]

    MAPPING = {
        'review': {
            'properties': {
                'business_id': {'index': 'not_analyzed', 'type': 'string'},
                'date': {'index': 'not_analyzed', 'format': 'dateOptionalTime', 'type': 'date'},
                'review_id': {'index': 'not_analyzed', 'type': 'string'},
                'stars': {'index': 'not_analyzed', 'type': 'integer'},
                'text': {
                    'index': 'analyzed',
                    'analyzer': 'english',
                    'store': 'yes',
                    "term_vector": "with_positions_offsets_payloads",
                    'type': 'string'},
                'fake_name': {'index': 'not_analyzed', 'type': 'string'},
                'text_orig': {'index': 'not_analyzed', 'type': 'string'},
                'user_id': {'index': 'not_analyzed', 'type': 'string'},
                'net_sentiment': {'index': 'not_analyzed', 'type': 'integer'},
                'sent_per_token': {'index': 'not_analyzed', 'type': 'float'}}}}

    if local_es.indices.exists('yelp'):
        local_es.indices.delete(index='yelp')
    local_es.indices.create(index='yelp')
    local_es.indices.put_mapping(index='yelp', doc_type='review', body=json.dumps(MAPPING))

    # Bulk data is structured as alternating opt_dict and data dicts.
    bulk_data = []

    for index, row in df.iterrows():
        data_dict = {}
        data_dict['text_orig'] = row['text']
        data_dict['text'] = row['text']
        data_dict['net_sentiment'] = row['net_sentiment']
        data_dict['sent_per_token'] = row['sent_per_token']
        data_dict['stars'] = row['stars']
        data_dict['fake_name'] = row['fake_name']
        data_dict['user_id'] = row['user_id']
        data_dict['business_id'] = row['business_id']
        data_dict['date'] = row['date']
        data_dict['review_id'] = row['review_id']
        op_dict = {
            "index": {
                "_index": 'yelp',
                "_type": 'review',
                "_id": row['review_id']}}
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

    pprint(bulk_data[0])
    pprint(bulk_data[1])
    print(len(bulk_data))

    # May time out with a large bulk_data bump or error and fail without any reason.  Mine did, so see below.
    # res = local_es.bulk(index = 'yelp', body = bulk_data)

    # In order to find the error, I did them one-by-one, with a try.
    for ind, obj in enumerate(bulk_data):
        # every other one is the data, so use those to do it one by one
        if ind % 2 != 0:
            try:
                local_es.index(index='yelp', doc_type='review', id=obj['review_id'], body=json.dumps(obj))
            except:
                print(obj)

    local_es.search(index='yelp', doc_type='review', q='pizza-cookie')

    # Remember that score relevancy results are based on the indexed TF-IDF for the doc and docs:
    #     https://www.elastic.co/guide/en/elasticsearch/guide/current/relevance-intro.html

    # Want to explain why something matched?  You need the id of the matched doc.
    local_es.explain(index='yelp', doc_type='review', q='pizza-cookie', id=u'fmn5yGrPChOYMR2vGOIrYA')

    # ### More Like This
    #
    # A variety of options for finding similar documents, including term counts and custom stop words:
    # https://www.elastic.co/guide/en/elasticsearch/reference/2.3/query-dsl-mlt-query.html
    #
    #
    text = df.iloc[0].text
    print(text)

    QUERY = {
        "query": {
            "more_like_this": {
                "fields": ["text"],
                "like_text": text,
                "analyzer": "english",
                "min_term_freq": 2}}}

    # Result is not brilliant, though.  You could limit the hits unless a score threshold is hit.
    pprint(local_es.search(index='yelp', doc_type='review', body=json.dumps(QUERY)))

    # ### Suggestions: For Mispellings
    #
    # Can be added to queries too, to help if there are no matches.  Still in development, though. See: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters.html#search-suggesters
    SUGGESTION = {
        "my-suggestion":
            {"text": "cheese piza",
             "term": {"field": "text"}}}

    # I don't love the results, tbh.  Fail on cheese.
    pprint(local_es.suggest(index='yelp', body=SUGGESTION))

    # ## Reminders:
    # * check your mapping on your fields
    # * check your analyzer results - they can be mysterious and hidden; if you configure wrong, it will use defaults...
    # * check your document tokenization
    # * use multi-fields to be sure of matches that may need stopwords too

    # ## Let's Index the Businesses too
    biz = pd.read_msgpack("data/biz_stats_df.msg")
    print(len(biz))
    pprint(biz[0:2])

    B_MAPPING = {
        'business': {
            'properties': {
                'business_id': {'index': 'not_analyzed', 'type': 'string'},
                'reviews': {'index': 'not_analyzed', 'type': 'integer'},
                'stars_median': {'index': 'not_analyzed', 'type': 'float'},
                'stars_mean': {'index': 'not_analyzed', 'type': 'float'},
                'text_length_median': {'index': 'not_analyzed', 'type': 'float'},
                'fake_name': {'index': 'not_analyzed', 'type': 'string'},
                'net_sentiment_median': {'index': 'not_analyzed', 'type': 'float'},
                'sent_per_token_median': {'index': 'not_analyzed', 'type': 'float'}}}}

    # local_es.indices.delete(index='yelp')  # nb: this errors the first time you run it. comment out.
    # local_es.indices.create(index='yelp')  # do not do this is you already made the reviews!
    local_es.indices.put_mapping(index='yelp', doc_type='business', body=json.dumps(B_MAPPING))

    bulk_data = []

    for index, row in biz.iterrows():
        data_dict = {}
        data_dict['net_sentiment_median'] = row['net_sentiment_median']
        data_dict['sent_per_token_median'] = row['sent_per_token_median']
        data_dict['stars_median'] = row['stars_median']
        data_dict['stars_mean'] = row['stars_mean']
        data_dict['fake_name'] = row['fake_name']
        data_dict['text_length_median'] = row['text_length_median']
        data_dict['business_id'] = row['business_id']
        data_dict['reviews'] = row['reviews']
        op_dict = {
            "index": {
                "_index": 'yelp',
                "_type": 'business',
                "_id": row['business_id']}}
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

    # May time out with a large bulk_data bump or error and fail without any reason.  Mine did, so see below.
    res = local_es.bulk(index='yelp', body=bulk_data)


    print(local_es.search(index='yelp', doc_type='business', q='JokKtdXU7zXHcr20Lrk29A'))

    # ## Aggregate Queries to get Business ID's and More
    #
    #
    # Here we are using the operator "and" to make sure all words in the search match, and then getting counts of matching business id's.
    QUERY = {
        "query": {
            "match": {
                "text": {
                    "query": "good pizza",
                    "operator": "and"
                }
            }
        },
        "aggs": {"businesses": {"terms": {"field": "business_id"}}}}

    pprint(local_es.search(index="yelp", doc_type="review", body=json.dumps(QUERY)))

    # exact match on field: https://www.elastic.co/guide/en/elasticsearch/guide/master/_finding_exact_values.html
    # requires not indexed field for the match
    QUERY = {
        "query": {
            "constant_score": {
                "filter": {
                    "term": {
                        "business_id": "VVeogjZya58oiTxK7qUjAQ"}}}}}

    pprint(local_es.search(index="yelp", doc_type="business", body=json.dumps(QUERY)))

    # ## Now Move to The JS App
    #
    # Now that you have the data indexed and searchable, we can build a small app to iterate on for the eventual ui you might want.  Use the "web" folder in my repo for that.
    #
    # ### Some Other Reference Materials:
    #
    # * Tutorial slides from PyCon 2015, repo here (may be out of date!): https://github.com/erikrose/elasticsearch-tutorial
    # * Docs for the Python Lib: https://elasticsearch-py.readthedocs.org/en/master/index.html
    #

if __name__ == '__main__':
    main()

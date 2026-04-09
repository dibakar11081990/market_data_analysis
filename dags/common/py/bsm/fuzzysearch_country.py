#############################################################
"""
Fuzzy Search for GEO Mapping
"""
# Imports
import pycountry
import string
import re
import jellyfish



# Stop words in countries
COUNTRY_STOP_WORDS = [
    'THE', 'OF', 'AND', 'ISLAMIC', 'BOLIVARIAN',
    'PLURINATIONAL', 'PEOPLES', 'DUTCH PART', 'FRENCH PART',
    'MALVINAS', 'YUGOSLAV','PROTECTORATE',
    'DEM', 'DEMOCRATIC', 'FMR', 'FORMER', 'PROV', 'PROVINCE',
    'REP', 'REPUBLIC', 'ST', 'SAINT', 'UTD', 'U',
    'N', 'NORTH', 'E', 'EAST', 'W', 'WEST', 'K', 'FED',
    'FEDERATION', 'FEDERAL', 'FEDERATED', 'ISL', 'ISLAND', 'ISLANDS',
    'S', 'TERR', 'TERRITORY', 'TERRITORIES', 'IS', 'SOUTHWEST', 'SOUTHEAST',
    'NORTHEAST', 'NORTHWEST']

# Special cases when the input string needs to be replaced altogether
COUNTRY_SPECIAL_HANDLING = {
    'UK': 'UNITED KINGDOM',
    'SWAZILAND': 'ESWATINI',
    'LAOS': "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
}

# LEVEINSTEIN Threshold. To make more strict approximate string matching reduce this threshold
THRESHOLD_LEVEINSTEIN_DISTANCE_RATIO = 0.25

#############################################################
# Functions


def remove_punctuation(sentence):
    """
    function which basically removes punctuations from a given sentences
    :param sentence: string from which punctuations needs to be removed.
    :return: string without punctuations
    """
    return re.sub("[{punctuation}]".format(punctuation=string.punctuation.replace("'", "")), ' ', sentence)



def preprocess(country_name):
    """
    Preprocess Country name String
    :param country_name: Country name which needs to be preprocessed.
    :return: preprocessed country name.
    """

    # convert in to lowercase
    country_name = country_name.lower()

    # comoros, f & i rep. => comoros
    index = country_name.find(',')
    if index != -1:
        country_name = country_name[:index]

    # barbados (BWI) => barbados
    country_name = re.sub('\(.+?\)', '', country_name)

    # Remove punctuations
    country_name = remove_punctuation(country_name.lower())

    # prepare regex based on stop words.
    stopwords = '|'.join(COUNTRY_STOP_WORDS)
    stopwords_regex = re.compile(r'\b(' + stopwords + r')\b', flags=re.IGNORECASE)

    # remove the stop words
    country_name = stopwords_regex.sub('', country_name)

    # trim the country
    country_name = country_name.strip()

    # return the final preprocessed country name.
    return country_name



def fuzzy_search_country(query):
    """
    simple fuzzy searching for country
    :param query: country name which needs to be fuzzy searched
    :return: triplet [ALPHA_2, ALPHA_3, COUNTRY]

    """

    # Call the function which performs preprocessing
    query = preprocess(country_name=query)

    # Special handling
    for key, value in COUNTRY_SPECIAL_HANDLING.items():
        if key.lower() == query.lower():
            query = value.lower()


    # A dictionary to hold the search results
    search_results = {}


    def add_result(country, points):
        """
        Inline function to update the result
        :param country: country for which the score needs to be updated
        :param points: points
        :return: nothing
        """
        search_results.setdefault(country.alpha_2, 0)
        search_results[country.alpha_2] += points


    # Prio 1: exact matches on country names
    try:
        # update the scores
        add_result(pycountry.countries.lookup(query), 50)
    except LookupError:
        pass


    # Prio 2: exact matches on subdivision names
    for candidate in pycountry.subdivisions:

        # get all fields values ['VE-J', 'Guárico', 'State', None, 'VE']
        for v in candidate._fields.values():

            # continue if v is none
            if v is None:
                continue

            # remove all the accents (diacritics)
            v = pycountry.remove_accents(v.lower())

            for v in v.split(';'):
                if v == query:
                    add_result(candidate.country, points=49)
                    break

    # Prio 3: partial matches on country names
    for candidate in pycountry.countries:

        # get the name / Official name for the country
        for v in [candidate._fields.get('name'), candidate._fields.get('official_name')]:

            # continue if v is none
            if v is None:
                continue

            # remove all the accents (diacritics)
            v = pycountry.remove_accents(v.lower())

            # if the  entire query is present in the v
            if query in v:
                add_result(candidate, points=48)
                break



    # Prio 4: EditDistance levenshtein_distance()
    results_edit_distance = {}
    for candidate in pycountry.countries:

        # cleanup the candidate country name
        candidate_cleaned_country = preprocess(country_name=candidate.name)

        # find the edit distance between candidate and given input string
        leveinstein_distance = jellyfish.levenshtein_distance(candidate_cleaned_country, query)

        # calculate the levenshtein_distance as a ratio
        results_edit_distance[candidate] = leveinstein_distance/len(query)

    # find the minimum distance (Country Object, ratio)
    min_dist = min(results_edit_distance.items(), key=lambda x: x[1])

    # if the ratio < threshold then add it to the result.
    if min_dist[1] < THRESHOLD_LEVEINSTEIN_DISTANCE_RATIO:
        add_result(min_dist[0], points=45)


    # TODO Prio 5: Jaro-Winkler Distance


    # Raise an exception if there was no match
    if not search_results:
        raise LookupError(query)


    # return the results sorted according to score.
    final_results = [pycountry.countries.get(alpha_2=x[0])
               for x in sorted(search_results.items(), key=lambda x: (-x[1], x[0]))]

    # return the final result
    return final_results


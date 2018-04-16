# -*- coding: utf-8 -*-
from fuzzywuzzy import fuzz
from rosette.api import API, NameSimilarityParameters


def fuzzy_match_with_rosette(s1, s2, thresh=0.7):
    key = '8de84cdb4432c800d3db01cc4e5d4de0'
    alt_url = 'https://api.rosette.com/rest/v1/'
    # Create an API instance
    api = API(user_key=key, service_url=alt_url)

    matched_name_data1 = s1
    matched_name_data2 = s2
    params = NameSimilarityParameters()
    params["name1"] = {"text": matched_name_data1, "language": "eng", "entityType": "PERSON"}
    params["name2"] = {"text": matched_name_data2, "entityType": "PERSON"}
    res = api.name_similarity(params)['score']
    return res > thresh


def fuzzy_match_with_fuzzywuzzy(s1, s2, thresh=0.7):
    return fuzz.ratio(s1, s2) > thresh * 100


def fuzzy_match(s1, s2, threshold=0.7, method='FUZZYWUZZY'):
    # if isinstance(s1, unicode):
    #     s1 = s1.encode('utf-8')
    # if isinstance(s1, unicode):
    #     s2 = s2.encode('utf-8')
    func = {
        'FUZZYWUZZY': fuzzy_match_with_fuzzywuzzy,
        'ROSETTE': fuzzy_match_with_rosette
    }[method]
    try:
        return func(s1, s2, threshold)
    except:
        # print('Error when fuzzy_match; just return False; %s' % traceback.format_exc())
        return False

# -*- coding: utf-8 -*-
import traceback
from multiprocessing import Pool, cpu_count
import json
from fuzzy_matching import fuzzy_match
from uuid import uuid4

class MatchLevel:
    CONFIDENT = 3
    INCONFIDENT = 2
    NOT_SURE = 1


class CompanyAliasGroup:
    def __init__(self):
        self.id = str(uuid4())
        #  all aliases should at least have the same parent_dns
        self.aliases = {MatchLevel.INCONFIDENT: [],
                        MatchLevel.CONFIDENT: []}  # company match_level: [list of aliases/company records]

    def match(self, company):
        # check how close a new company record is to this alias group
        if company[3] != self.aliases[MatchLevel.CONFIDENT][0][3]:
            return MatchLevel.NOT_SURE
        # only compare the new company record with existing "confident" aliases.
        match_level = max(match_two_companies(company, a) for a in self.aliases[MatchLevel.CONFIDENT])
        return match_level

    def add_alias(self, company, match_level):
        if match_level == MatchLevel.NOT_SURE:
            return
        self.aliases[match_level].append(company)

    def get_aliases(self, match_level=None):
        if match_level is None:
            return self.aliases
        return self.aliases.get(match_level)

    def set_alias(self, alias_json):
        self.aliases = alias_json

    def set_id(self, id):
        self.id = id

    def get_id(self):
        return self.id


def match_two_companies(company1, company2):
    """
    company1 and 2 are tuples containing each company's detailed information
    it returns CONFIDENT/INCONFIDENT if two companies are deemed as the same company, else returns NOT_MATCH
    #######################################################################################################
    1. if two companies' parent duns are not the same, return NOT_SURE

    2. if two companies' duns ids are the same, return CONFIDENT (we're confident that they're considered as the same company)
    3. if two companies' names are the same, return CONFIDENT
    4. if two companies' zip 4 codes are the same, return CONFIDENT.

    5. if two companies' names "fuzzy match", return INCONFIDENT (they're very likely to be the same company).
        for fuzzy matching, we use Rosette-api. https://developer.rosette.com/features-and-functions#introduction
        we can actually use other better proprietary apis for real enterprise use.

    6. else return NOT_SURE. they may be, may not be the same, but we don't know
    """

    if company1[3] != company2[3]:  # parent duns are not the same, they're two separate companies
        return MatchLevel.NOT_SURE

    if company1[0] == company2[0]:  # duns
        return MatchLevel.CONFIDENT

    if company1[1] == company2[1]:  # companies' names
        return MatchLevel.CONFIDENT

    if company1[8] == company2[8]:  # zip4code
        return MatchLevel.CONFIDENT

    if fuzzy_match(company1[1], company2[1]):
        return MatchLevel.INCONFIDENT

    return MatchLevel.NOT_SURE


def get_company_alias_groups_per_parent_duns(args):
    try:
        uniq_records, allow_inconfident_match = args
        company_alias_groups = []  # a list of CompanyAliasGroup instances
        for record in uniq_records:
            for grp in company_alias_groups:
                match_level = grp.match(record)
                if (match_level == MatchLevel.CONFIDENT) or (
                    allow_inconfident_match and match_level == MatchLevel.INCONFIDENT):
                    grp.add_alias(record, match_level)
                    break
            else:
                #  if no break in for loop, it means this record doesn't belong to any existing alias group and we should create a new group
                new_alias_group = CompanyAliasGroup()
                new_alias_group.add_alias(record, MatchLevel.CONFIDENT)  # the first/root alias in this new group
                company_alias_groups.append(new_alias_group)
        # return list of lists. each sub-list contains all the aliases of a company
        return company_alias_groups, None
    except Exception as e:
        print('Error when get_company_alias_groups_per_parent_duns: %s' % traceback.format_exc())
        return None, e


def get_company_alias_groups(uniq_records, allow_inconfident_match=False):
    '''
    group all records by parent duns, because two companies with different parent duns are definitely two
    different company (not aliases)
    use multiprocessing on function get_company_alias_groups_per_parent_duns to speed up
    '''
    pool = None
    try:
        record_grpby_parent_duns = {}
        for r in uniq_records:
            if r[3] not in record_grpby_parent_duns:
                record_grpby_parent_duns[r[3]] = []
            record_grpby_parent_duns[r[3]].append(r)
        alias_groups = []
        pool = Pool(cpu_count())
        for alias_group_per_parent_duns, err in pool.imap_unordered(get_company_alias_groups_per_parent_duns,
                                                                    ((records, allow_inconfident_match) for records in
                                                                     record_grpby_parent_duns.values())):
            if err:
                raise err
            alias_groups.extend(alias_group_per_parent_duns)
        result = {grp.get_id(): grp.get_aliases() for grp in alias_groups}
        print("Finished grouping company aliases. %d unique alias groups (unique companies) found" % len(result))
        return result
    except:
        if pool is not None:
            pool.close()
        print('Error when getting company alias groups: %s' % traceback.format_exc())


class AliasGroupFinder:
    def __init__(self, file_name):
        self.alias_groups = self.load_alias_groups_from_file(file_name)
        self.alias_groups_by_parent_duns = self.index_alias_groups_by_parent_duns()

    def load_alias_groups_from_file(self, file_name):
        alias_gropus = []
        with open(file_name, 'rb') as f:
            json_body = json_loads_byteified(f.read())
            for id, grp in json_body.iteritems():
                alias_group = CompanyAliasGroup()
                alias_group.set_alias({int(k): v for k, v in grp.items()})
                alias_group.set_id(id)
                alias_gropus.append(alias_group)
            return alias_gropus

    def index_alias_groups_by_parent_duns(self):
        groups_by_pduns = {}
        for grp in self.alias_groups:
            pdun = grp.get_aliases()[MatchLevel.CONFIDENT][0][3]
            if pdun not in groups_by_pduns:
                groups_by_pduns[pdun] = []
            groups_by_pduns[pdun].append(grp)
        return groups_by_pduns

    def find_alias_group_by_company_record(self, company, match_level=MatchLevel.CONFIDENT):
        pduns = company[3]
        for grp in self.alias_groups_by_parent_duns.get(pduns, []):
            if grp.match(company) >= match_level:
                return grp.get_id(), grp.get_aliases(match_level)
        return None


def json_loads_byteified(json_text):
    return _byteify(
        json.loads(json_text, object_hook=_byteify),
        ignore_dicts=True
    )

def _byteify(data, ignore_dicts = False):
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [ _byteify(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data
import json
import csv
import random

from company_alias_grouper import get_company_alias_groups, AliasGroupFinder
from data_wrangler import wrangle_data, dedup_exactly_same_records
import classify_all_original_records

def main():
    #  extract interesting columns and write them into a new csv file
    # wrangle_data(['contracts_prime_transactions_%d.csv' % d for d in range(1, 6)], final_csv_file='final.csv')
    # uniq_records = dedup_exactly_same_records('final.csv', 'dedup_final.csv')
    # dedup those records that are exactly the same (dup definition: every column of one row equals the other row's)
    # with open('dedup_final.csv', 'rb') as rf:
    #     rf.readline()
    #     csv_reader = csv.reader(rf, delimiter=',')
    #     uniq_records = map(tuple, csv_reader)
    #     sample_companies = random.sample(uniq_records, 10)
    #
    # confident_company_aliases = get_company_alias_groups(uniq_records, False)
    # with open('confident_company_aliases', 'wb') as f:
    #     f.write(json.dumps(confident_company_aliases))
    #
    # inconfident_company_aliases = get_company_alias_groups(uniq_records, True)
    # with open('inconfident_company_aliases', 'wb') as f:
    #     f.write(json.dumps(inconfident_company_aliases))

    # alias_finder = AliasGroupFinder('confident_company_aliases')
    # for company in sample_companies:
    #     alias_group_id, aliases = alias_finder.find_alias_group_by_company_record(company)
    #     print('It matches alias group %s. We found those records that may represent the same company you provided: %s' % (alias_group_id, json.dumps(aliases)))
    classify_all_original_records.multipro_add_group_id(['contracts_prime_transactions_%d.csv' % d for d in range(1, 6)], final_csv_file='final.csv')


if __name__ == '__main__':
    main()



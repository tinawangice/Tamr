# -*- coding: utf-8 -*-
import csv
import os
import traceback
import time
from multiprocessing import Pool, cpu_count
from company_alias_grouper import get_company_alias_groups, AliasGroupFinder
from data_wrangler import raw_cols_to_interested_cols, row_2_cleaned_row
from uuid import uuid4
import json


def add_group_id(src_csv_file, match_file ='confident_company_aliases'):
    dst_csv_file = src_csv_file.replace('.csv', '') + "_with_group_id.csv"
    # interesting_col_ids = set(excel_col_to_num(col) for col, _ in INTERESTING_COLUMNS)
    alias_finder = AliasGroupFinder(match_file)
    try:
        with open(dst_csv_file, 'wb') as wf:
            csv_writer = csv.writer(wf, delimiter=',')
            with open(src_csv_file, 'rb') as rf:
                header = rf.readline()
                csv_reader = csv.reader(rf, delimiter=',')
                t0 = 0
                for idx, row in enumerate(csv_reader):
                    if idx == 0:
                        t0 = time.time()
                    if idx > 0 and idx%2000==0:
                        print(idx, time.time() - t0)
                        t0 = time.time()
                    # shorter_row = [c for cidx, c in enumerate(row) if cidx in interesting_col_ids]
                    shorter_row = raw_cols_to_interested_cols(row)
                    # cleaned_row = tuple(map(lambda c: c.strip().upper(), shorter_row))
                    cleaned_row = row_2_cleaned_row(shorter_row)
                    alias_group_id, aliases = alias_finder.find_alias_group_by_company_record(cleaned_row)
                    csv_writer.writerow([uuid4()]+row + [alias_group_id])

        return src_csv_file, dst_csv_file, None
    except Exception as e:
        print('Error when adding group id from %s; %s' % (src_csv_file, traceback.format_exc()))
        return src_csv_file, dst_csv_file, e


def multipro_add_group_id(csv_files, final_csv_file):
    # convert multiple csv files downloaded from www.usaspending.gov into new csv files with interesting columns only
    pool = None
    try:
        pool = Pool(cpu_count())
        header_line_written = False
        with open(final_csv_file, 'wb') as wf:
            for src, dst, err in pool.imap_unordered(add_group_id, csv_files):
                if not err:
                    print('successfully converted %s into %s' % (src, dst))
                    with open(dst, 'rb') as rf:
                        hl = rf.readline()
                        if not header_line_written:
                            wf.write(hl)
                            header_line_written = True
                        wf.writelines(rf)
                    os.remove(dst)
    except:
        # this shouldn't happen
        print('wrangle_data: Error happened: %s' % traceback.format_exc())
        raise
    finally:
        if pool is not None:
            pool.close()

def run():
    #  extract interesting columns and write them into a new csv file

    # uniq_records = dedup_exactly_same_records('final.csv', 'dedup_final.csv')
    # dedup those records that are exactly the same (dup definition: every column of one row equals the other row's)

    # confident_company_aliases = get_company_alias_groups(uniq_records, False)
    # with open('confident_company_aliases', 'wb') as f:
    #     f.write(json.dumps(confident_company_aliases))

    # inconfident_company_aliases = get_company_alias_groups(uniq_records, True)
    multipro_add_group_id(['contracts_prime_transactions_%d.csv' % d for d in range(1, 6)], final_csv_file='data_with_group_id.csv')

if __name__ == '__main__':
    run()
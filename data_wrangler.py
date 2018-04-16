import csv
import os
import traceback
from multiprocessing import Pool, cpu_count

from utils import excel_col_to_num

INTERESTING_COLUMNS = [('AD', 'recipient_duns'), ('AE', 'recipient_name'), ('AG', 'recipient_parent_name'),
                       ('AH', 'recipient_parent_duns'), ('AI', 'recipient_country_code'),
                       ('AK', 'recipient_address_line_1'), ('AN', 'recipient_city_name'),
                       ('AO', 'recipient_state_code'), ('AQ', 'recipient_zip_4_code'), ('AS', 'recipient_phone_number'),
                       ('AT', 'recipient_fax_number')]


def raw_cols_to_interested_cols(row):
    interesting_col_ids = set(excel_col_to_num(col) for col, _ in INTERESTING_COLUMNS)
    shorter_row = [c for cidx, c in enumerate(row) if cidx in interesting_col_ids]
    return shorter_row


def row_2_cleaned_row(row):
    return tuple(map(lambda c: c.strip().upper(), row))


def wrangle_data(csv_files, final_csv_file):
    # convert multiple csv files downloaded from www.usaspending.gov into new csv files with interesting columns only
    pool = None
    try:
        pool = Pool(cpu_count())
        header_line_written = False
        with open(final_csv_file, 'wb') as wf:
            for src, dst, err in pool.imap_unordered(extract_interesting_columns, csv_files):
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


def dedup_exactly_same_records(csv_file, dedupped_csv_file):
    # dedup rows with exactly same columns
    uniq_keys = set()
    original_count = 0
    with open(csv_file, 'rb') as f:
        header = f.readline()
        csv_reader = csv.reader(f, delimiter=',')
        for r in csv_reader:
            if r:
                original_count += 1
                # if original_count % 2000 == 0 and original_count:
                #     print('Read %d records' % original_count)
                # cleaned_row = tuple(map(lambda c: c.strip().upper(), r))
                cleaned_row = row_2_cleaned_row(r)
                # make row into tuple and use it as key for dedup
                uniq_keys.add(cleaned_row)
    with open(dedupped_csv_file, 'wb') as wf:
        wf.write(header)
        csv_writer = csv.writer(wf, delimiter=',')
        for record in uniq_keys:
            csv_writer.writerow(record)
    print(
            'Finished dedupping records. Read %d records and got %s records finally. Unique records were written to file %s' % (
        original_count, len(uniq_keys), dedupped_csv_file))
    return list(uniq_keys)


def extract_interesting_columns(src_csv_file):
    dst_csv_file = src_csv_file.replace('.csv', '') + "_short.csv"
    # interesting_col_ids = set(excel_col_to_num(col) for col, _ in INTERESTING_COLUMNS)
    try:
        with open(dst_csv_file, 'wb') as wf:
            csv_writer = csv.writer(wf, delimiter=',')
            with open(src_csv_file, 'rb') as rf:
                csv_reader = csv.reader(rf, delimiter=',')
                for row in csv_reader:
                    # shorter_row = [c for cidx, c in enumerate(row) if cidx in interesting_col_ids]
                    shorter_row = raw_cols_to_interested_cols(row)
                    csv_writer.writerow(shorter_row)
        return src_csv_file, dst_csv_file, None
    except Exception as e:
        print('Error when extrating columns from %s; %s' % (src_csv_file, traceback.format_exc()))
        return src_csv_file, dst_csv_file, e

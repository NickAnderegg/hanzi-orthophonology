import pathlib
import mysql.connector as mariadb
import time

timestamp = time.perf_counter()
print('Opening database...')
mariadb_connection = mariadb.connect(user='root', passwd='pN^E!2Rgm1TZ@KD&06wIKK4gq', host='192.168.1.150', port='3306', database='orthophonology')
mariadb_connection.get_warnings = True
cursor = mariadb_connection.cursor()


    # ALTER TABLE word_ortho_comps_twochar DISABLE KEYS;
    # SET @@session.unique_checks = 0;
    # SET @@session.foreign_key_checks = 0;

print('Acquiring file names...')
p = pathlib.Path('/Volumes/research/orthophonology/word_orthography/session5/')

print('Processing files...')
count = 0
runtime = 0

paths = []
for x in p.iterdir():
    if x.is_file() and '.csv' in x.suffixes:
        paths.append(x.name)
total = len(paths)

print('Starting insertion...')
for x in paths:
    timestamp = time.perf_counter()
    cursor.execute('''
            LOAD DATA INFILE "/research/orthophonology/word_orthography/session5/{}"
            INTO TABLE word_ortho_comps_twochar_freq50
            COLUMNS TERMINATED BY ","
            LINES TERMINATED BY "\\n"
            (word1, word2, sim);
        '''.format(x))
    mariadb_connection.commit()

    count += 1
    process_time = time.perf_counter() - timestamp
    runtime += process_time
    avg_time = runtime/count
    est_remaining = (total - count) * avg_time
    print('File "{}" processed.\tDuration: {}'.format(x, process_time))
    print('Total processed: {} ({:.2%}) in {:0>2}:{:0>2}:{:0>2} (Avg: {:.1f}s / Est. remaining: {:0>2}:{:0>2}:{:0>2})'.format(count, (count/total), int(runtime/3600), int((runtime%3600)/60), int(runtime%60), avg_time, int(est_remaining/3600), int((est_remaining%3600)/60), int(est_remaining%60)))

mariadb_connection.close()

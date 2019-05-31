#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================
#       Set-up
# ====================

# Import required modules
import datetime
from fuzzywuzzy import fuzz
import gc
import glob
import os
import re
import sqlite3
import sys
from identities_tools.isbn_tools import *
from identities_tools.marc_tools import *


__author__ = 'Victoria Morris'
__license__ = 'MIT License'
__version__ = '1.0.0'
__status__ = '4 - Beta Development'


# ====================
#      Constants
# ====================

DATABASE_PATH = 'identities_graph.db'

DUMP_FILE_PATH = os.path.join(os.getcwd(), 'Data\\DUMP')
DUMP_FILE_PATTERN = '*.tsv'

TSV_FILE_PATH = os.path.join(os.getcwd(), 'Data\\TSV')
TSV_FILE_PATTERN = '*.tsv'

ISBN_FILE_PATH = os.path.join(os.getcwd(), 'Data\\ISBN')
ISBN_FILE_PATTERN = '*.txt'

VIAF_TABLE_PATH = os.path.join(os.getcwd(), 'Data\\VIAF')
VIAF_TABLE_PATTERN = 'viaf*-links.txt'
VIAF_FILE_PATH = os.path.join(os.getcwd(), 'Data\\VIAF')
VIAF_FILE_PATTERN = 'viaf*-marc21.lex'

NACO_FILE_PATH = os.path.join(os.getcwd(), 'Data\\NACO')
NACO_FILE_PATTERN = 'naco*.lex'
BNB_FILE_PATH = os.path.join(os.getcwd(), 'Data\\BNB')
BNB_FILE_PATTERN = '*-bnb.mrc'

NODE_TYPES = ['string', 'isbn', 'isni', 'viaf', 'naco', 'harpercollins', 'penguin', 'randomhouse']

IDENTIFIER_PAIRS = [('naco', 'isni'), ('naco', 'harpercollins'), ('naco', 'penguin'), ('naco', 'randomhouse'),
                    ('isni', 'harpercollins'),  ('isni', 'penguin'),  ('isni', 'randomhouse')]

GRAPH_TABLES = {
    'NACO_authorised': ([
        ('NACO', 'TEXT'),
        ('string', 'TEXT'),
    ]),
    'NACO_variants': ([
        ('NACO', 'TEXT'),
        ('string', 'TEXT'),
    ]),
    'VIAF_equivalences': ([
        ('VIAF', 'TEXT'),
        ('identifier', 'TEXT'),
    ]),
    'other_equivalences': ([
        ('other', 'TEXT'),
        ('identifier', 'TEXT'),
    ]),     # Can be NACO-ISNI, NACO-HarperCollins, NACO-Penguin, NACO-RandomHouse,
            #                   ISNI-HarperCollins, ISNI-Penguin, ISNI-RandomHouse,
    'VIAF_isbn': ([
        ('VIAF', 'TEXT'),
        ('isbn', 'NCHAR(13)'),
    ]),
    'other_isbn': ([
        ('other', 'TEXT'),
        ('isbn', 'NCHAR(13)'),
    ]),     # Can be NACO-isbn, ISNI-isbn, HarperCollins-isbn, Penguin-isbn, RandomHouse-isbn
    'VIAF_string': ([
        ('VIAF', 'TEXT'),
        ('string', 'TEXT'),
    ]),
    'string_isbn': ([
        ('string', 'TEXT'),
        ('isbn', 'NCHAR(13)'),
    ]),
    'isbn_equivalents': ([
        ('isbna', 'NCHAR(13)'),
        ('isbnb', 'NCHAR(13)'),
    ]),
}


# ====================
#      Functions
# ====================




# ====================
#       Classes
# ====================


class TSV:

    def __init__(self, string, headers):
        self.identifiers = {a: set() for a in NODE_TYPES}
        entries = string.split('\t')
        for j, val in enumerate(entries):
            try:
                _, h = headers[j]
            except Exception as e:
                print('ERROR' + str(j) + '|' + str(e))
                continue
            val = re.sub(r'^(isni|viaf|naco|harpercollins|penguin|randomhouse):', '', val.strip('"').strip()).strip()
            h = which(h.lower(), NODE_TYPES)
            if not h: continue
            if h in ['isni', 'viaf', 'naco']:
                val = clean_identifier(val, type=h)
                if not is_null(val): self.identifiers[h].add(val)
                continue
            if h in ['string', 'harpercollins', 'penguin', 'randomhouse']:
                if not is_null(val): self.identifiers[h].add(val)
                continue
            if h == 'isbn':
                val = str(Isbn(val))
                if not is_null(val): self.identifiers['isbn'].add(val)
        del entries

    def get_identifiers(self):
        return self.identifiers

    def get_isbns(self):
        return self.identifiers['isbn']

    def get_names(self):
        return self.identifiers['string']

    def get_proprietary(self):
        for h in ['harpercollins', 'penguin', 'randomhouse']:
            for val in self.identifiers[h]:
                if val: return val
        return None

    def get(self):
        return self.identifiers


class IdentityGraphDatabase:

    def __init__(self):
        # Connect to database
        print('\n\nConnecting to local database ...')
        print('----------------------------------------')
        print(str(datetime.datetime.now()))

        self.conn = sqlite3.connect(DATABASE_PATH)
        self.cursor = self.conn.cursor()

        # Set up database
        self.cursor.execute('PRAGMA synchronous = OFF')
        self.cursor.execute('PRAGMA journal_mode = OFF')
        self.cursor.execute('PRAGMA locking_mode = EXCLUSIVE')
        self.cursor.execute('PRAGMA count_changes = FALSE')
        self.cursor.execute("PRAGMA temp_store_directory = 'I:\Temp'")

        # Create tables
        for table in GRAPH_TABLES:
            print('Creating table {} ...'.format(table))
            self.cursor.execute('CREATE TABLE IF NOT EXISTS {} '
                                '({}, UNIQUE({}));'
                                .format(table, ', '.join('{} {}'.format(key, value) for (key, value) in GRAPH_TABLES[table]),
                                        ', '.join(key for (key, value) in GRAPH_TABLES[table])))
        self.conn.commit()
        gc.collect()

    def close(self):
        self.conn.close()
        gc.collect()

    def clean(self):
        date_time_message('Cleaning')

        self.cross_reference()

        # Delete null entries
        for table in GRAPH_TABLES:
            print('Deleting NULL entries from table {} ...'.format(table))
            self.cursor.execute('DELETE FROM {} '
                                'WHERE {} IS NULL OR {} IS NULL OR {} = "" OR {} = "" ;'
                                .format(table, GRAPH_TABLES[table][0][0], GRAPH_TABLES[table][1][0], GRAPH_TABLES[table][0][0], GRAPH_TABLES[table][1][0]))
        self.conn.commit()
        gc.collect()

        date_time_message('Vacuuming')
        self.conn.execute("VACUUM")
        self.conn.commit()
        gc.collect()

    def cross_reference(self):
        print('Cross-referencing tables VIAF_equivalences and other_equivalences ...')
        self.cursor.execute('INSERT OR IGNORE INTO VIAF_equivalences (VIAF, identifier) '
                            'SELECT VIAF_equivalences.VIAF, other_equivalences.identifier '
                            'FROM VIAF_equivalences INNER JOIN other_equivalences ON VIAF_equivalences.identifier = other_equivalences.other  ;')
        self.conn.commit()
        self.cursor.execute('INSERT OR IGNORE INTO VIAF_equivalences (VIAF, identifier) '
                            'SELECT VIAF_equivalences.VIAF, other_equivalences.other '
                            'FROM VIAF_equivalences INNER JOIN other_equivalences ON VIAF_equivalences.identifier = other_equivalences.identifier  ;')
        self.conn.commit()
        self.cursor.execute('DELETE FROM other_equivalences '
                            'WHERE other_equivalences.identifier IN '
                            '(SELECT identifier FROM VIAF_equivalences) ;')
        self.conn.commit()
        self.cursor.execute('DELETE FROM other_equivalences '
                            'WHERE other_equivalences.other IN '
                            '(SELECT identifier FROM VIAF_equivalences) ;')
        self.conn.commit()
        gc.collect()
        print('Cross-referencing tables VIAF_isbn, VIAF_equivalences and other_isbn ...')
        self.cursor.execute('INSERT OR IGNORE INTO VIAF_isbn (VIAF, isbn) '
                            'SELECT VIAF_equivalences.VIAF, other_isbn.isbn '
                            'FROM VIAF_equivalences INNER JOIN other_isbn ON VIAF_equivalences.identifier = other_isbn.other  ;')
        self.conn.commit()
        self.cursor.execute('DELETE FROM other_isbn '
                            'WHERE other_isbn.other IN '
                            '(SELECT identifier FROM VIAF_equivalences) ;')
        self.conn.commit()
        gc.collect()

    def set_queries(self):
        queries, values = {}, {}
        for table in GRAPH_TABLES:
            queries['{}'.format(table)] = 'INSERT OR IGNORE INTO {} ({}) VALUES (?, ?);'.format(table, ', '.join(
                key for (key, value) in GRAPH_TABLES[table]))
            values['{}'.format(table)] = []
        return queries, values

    def execute_all(self, query, values):
        if values:
            self.cursor.executemany(query, values)
            self.conn.commit()
            gc.collect()
        return []

    def build_index(self, table):
        """Function to build indexes in a table"""
        if table not in GRAPH_TABLES:
            print('Table name {} not recognised'.format(table))
            return None
        print('\nBuilding indexes in {} table ...'.format(table))

        self.cursor.execute("""DROP INDEX IF EXISTS IDX_{}_0 ;""".format(table))
        self.cursor.execute("""CREATE INDEX IDX_{}_0 ON {} ({});""".format(table, table, GRAPH_TABLES[table][0][0]))
        self.cursor.execute("""DROP INDEX IF EXISTS IDX_{}_1 ;""".format(table))
        self.cursor.execute("""CREATE INDEX IDX_{}_1 ON {} ({});""".format(table, table, GRAPH_TABLES[table][1][0]))
        self.conn.commit()
        gc.collect()

    def build_indexes(self):
        """Function to build indexes in the whole database"""
        print('\nBuilding indexes ...')
        print('----------------------------------------')
        print(str(datetime.datetime.now()))

        for table in GRAPH_TABLES:
            self.build_index(table)

    def drop_indexes(self):
        """Function to drop indexes in the whole database"""
        for table in GRAPH_TABLES:
            self.cursor.execute("""DROP INDEX IF EXISTS IDX_{}_0 ;""".format(table))
            self.cursor.execute("""DROP INDEX IF EXISTS IDX_{}_1 ;""".format(table))
            self.conn.commit()
        gc.collect()

    def dump_table(self, table):
        """Function to dump a database table into a text file"""
        print('Creating dump of {} table ...'.format(table))
        self.cursor.execute('SELECT * FROM {};'.format(table))
        file = open('{}_DUMP_.txt'.format(table), mode='w', encoding='utf-8', errors='replace')
        record_count = 0
        row = self.cursor.fetchone()
        while row:
            record_count += 1
            if record_count % 100 == 0:
                print('\r{} records processed'.format(str(record_count)), end='\r')
            file.write('{}\n'.format(str(row)))
            row = self.cursor.fetchone()
        del row
        print('\r{} records processed'.format(str(record_count)), end='\r')
        file.close()
        gc.collect()
        print('{} records in {} table'.format(str(record_count), table))
        return record_count

    def dump_database(self):
        """Function to create dumps of all tables within the database"""
        print('\nCreating dump of database ...')
        print('----------------------------------------')
        print(str(datetime.datetime.now()))

        for table in GRAPH_TABLES:
            self.dump_table('{}'.format(table))

    def create_temp_table(self, columns=('string', 'isbn', 'identifier')):
        self.cursor.execute('DROP TABLE IF EXISTS ttable ;')
        self.cursor.execute('CREATE TABLE ttable ({} TEXT, {} TEXT, {} TEXT) ;'.format(columns[0], columns[1], columns[2]))
        self.conn.commit()

    def add_values(self, identifiers, names, values):

        if len(identifiers['viaf']) > 0:
            for v in identifiers['viaf']:
                for h in ['naco', 'isni', 'harpercollins', 'penguin', 'randomhouse']:
                    for i in identifiers[h]:
                        values['VIAF_equivalences'].append(('viaf:{}'.format(v), '{}:{}'.format(h, i)))
                for isbn in identifiers['isbn']:
                    values['VIAF_isbn'].append(('viaf:{}'.format(v), isbn))
        else:
            for (h, k) in IDENTIFIER_PAIRS:
                for i in identifiers[h]:
                    for j in identifiers[k]:
                        values['other_equivalences'].append(('{}:{}'.format(h, i), '{}:{}'.format(k, j)))
            for h in ['naco', 'isni', 'harpercollins', 'penguin', 'randomhouse']:
                for i in identifiers[h]:
                    for isbn in identifiers['isbn']:
                        values['other_isbn'].append(('{}:{}'.format(h, i), isbn))

        for name in names:
            for isbn in identifiers['isbn']:
                values['string_isbn'].append((name, isbn))
            for v in identifiers['viaf']:
                values['VIAF_string'].append(('viaf:{}'.format(v), name))

        return values

    def add_marc(self, record_type='BNB'):
        """Function to add data from MARC files"""

        # This currently only works for NACO records, which do not contain ISBNs

        if record_type == 'NACO':
            file_list = glob.glob('\\'.join((NACO_FILE_PATH, NACO_FILE_PATTERN)))
        elif record_type == 'VIAF':
            file_list = glob.glob('\\'.join((VIAF_FILE_PATH, VIAF_FILE_PATTERN)))
        else:
            file_list = glob.glob('\\'.join((BNB_FILE_PATH, BNB_FILE_PATTERN)))

        for file in file_list:
            queries, values = self.set_queries()

            print('\n\nParsing {} file {} ...'.format(record_type, str(file)))
            print('----------------------------------------')
            print(str(datetime.datetime.now()))

            record_count = 0
            file = open(file, mode='rb')
            reader = MARCReader(file)
            for record in reader:
                record_count += 1
                identifiers = record.get_identifiers(record_type=record_type)
                names = record.get_name_strings()

                if record_type == 'NACO':
                    authorised_name = record.get_authorised_name()
                    if not authorised_name: continue
                    for n in identifiers['naco']:
                        values['NACO_authorised'].append((n, authorised_name))
                        for name in names:
                            if name == authorised_name: continue
                            values['NACO_variants'].append((n, name))

                values = self.add_values(identifiers, names, values)

                if record_count % 10000 == 0:
                    print('\r{} records processed'.format(str(record_count)), end='\r')
                    for v in queries:
                        values[v] = self.execute_all(queries[v], values[v])

                del identifiers, names
            file.close()
            print('\r{} records processed'.format(str(record_count)), end='\r')
            for v in queries:
                self.execute_all(queries[v], values[v])
        self.clean()
        del file_list

    def add_tsv(self):
        """Function to add data from TSV files"""
        file_list = glob.glob('\\'.join((TSV_FILE_PATH, TSV_FILE_PATTERN)))
        for file in file_list:
            queries, values = self.set_queries()

            print('\nAdding records from file {} ...'.format(str(file)))
            print('----------------------------------------')
            print(str(datetime.datetime.now()))

            file = open(file, mode='r', encoding='utf-8', errors='replace')
            headers = list(enumerate(file.readline().split('\t')))
            record_count = 0

            for filelineno, line in enumerate(file):
                record_count += 1

                tsv = TSV(line.strip('\n'), headers)
                identifiers = tsv.get()
                names = tsv.get_names()
                if not identifiers:
                    continue

                values = self.add_values(identifiers, names, values)

                if record_count % 1000 == 0:
                    print('\r{} records processed'.format(str(record_count)), end='\r')
                    for v in queries:
                        values[v] = self.execute_all(queries[v], values[v])

            file.close()
            print('\r{} records processed'.format(str(record_count)), end='\r')
            for v in queries:
                self.execute_all(queries[v], values[v])
        self.clean()

    def add_viaf_links(self):
        """Function to add data from VIAF links table"""
        file_list = glob.glob('\\'.join((VIAF_TABLE_PATH, VIAF_TABLE_PATTERN)))
        for file in file_list:
            queries, values = self.set_queries()

            print('\n\nParsing VIAF links table from file {} ...'.format(str(file)))
            print('----------------------------------------')
            print(str(datetime.datetime.now()))

            file = open(file, mode='r', encoding='utf-8', errors='replace')
            record_count = 0

            for filelineno, line in enumerate(file):
                record_count += 1

                if '@' in line or '|' not in line: continue
                if '\tISNI|' not in line and '\tLC|' not in line: continue
                viaf, other = line.strip().split('\t')
                viaf = clean_identifier(viaf.replace('http://viaf.org/viaf/', ''), type='viaf')
                other_type, other = other.split('|')
                if other_type == 'LC':
                    other = clean_identifier(other, type='naco')
                    values['VIAF_equivalences'].append(('viaf:{}'.format(viaf), 'naco:{}'.format(other)))
                elif other_type == 'ISNI':
                    other = clean_identifier(other, type='isni')
                    values['VIAF_equivalences'].append(('viaf:{}'.format(viaf), 'isni:{}'.format(other)))
                if record_count % 10000 == 0:
                    print('\r{} records processed'.format(str(record_count)), end='\r')
                    for v in queries:
                        values[v] = self.execute_all(queries[v], values[v])
                del viaf, other, other_type
            file.close()
            print('\r{} records processed'.format(str(record_count)), end='\r')
            for v in queries:
                self.execute_all(queries[v], values[v])
            self.clean()

    def add_isbns(self):
        """Function to add ISBN equivalences"""
        file_list = glob.glob('\\'.join((ISBN_FILE_PATH, ISBN_FILE_PATTERN)))
        for file in file_list:

            print('\n\nParsing ISBN equivalences from file {} ...'.format(str(file)))
            print('----------------------------------------')
            print(str(datetime.datetime.now()))

            query = 'INSERT OR IGNORE INTO isbn_equivalents (isbna, isbnb) VALUES (?, ?);'
            values = []

            file = open(file, mode='r', encoding='utf-8', errors='replace')
            record_count = 0

            for filelineno, line in enumerate(file):
                record_count += 1
                _, isbna, _, isbnb, _ = line.split('\'')
                values.append((isbna, isbnb))
                values.append((isbnb, isbna))
                values.append((isbna, isbna))
                values.append((isbnb, isbnb))
                if record_count % 1000 == 0:
                    print('\r{} records processed'.format(str(record_count)), end='\r')
                    self.execute_all(query, values)
            file.close()
            print('\r{} records processed'.format(str(record_count)), end='\r')
            self.execute_all(query, values)
            self.clean()

    def find_name_matches(self):
        """Function to find matching names"""
        file_list = glob.glob('\\'.join((TSV_FILE_PATH, TSV_FILE_PATTERN)))
        for file in file_list:

            print('\nSearching file {} for name matches ...'.format(str(file)))
            print('----------------------------------------')
            print(str(datetime.datetime.now()))

            self.create_temp_table()
            query = 'INSERT INTO ttable (string, isbn, identifier) VALUES (?, ?, ?);'
            values = []

            filename, _ = os.path.splitext(os.path.basename(file))
            file = open(file, mode='r', encoding='utf-8', errors='replace')
            headers = list(enumerate(file.readline().split('\t')))
            record_count = 0

            for filelineno, line in enumerate(file):
                record_count += 1

                tsv = TSV(line.strip('\n'), headers)
                names = tsv.get_names()
                isbns = tsv.get_isbns()
                proprietary = tsv.get_proprietary()
                if names and isbns:
                    for name in names:
                        for isbn in isbns:
                            values.append((name, isbn, proprietary))

                if record_count % 100 == 0:
                    print('\r{} records processed'.format(str(record_count)), end='\r')
                    values = self.execute_all(query, values)

            self.execute_all(query, values)

            print('\nSearching for name matches ...')

            file_accept = open('{}_name_list_accepted.txt'.format(filename), 'w', encoding='utf-8', errors='replace')
            file_reject = open('{}_name_list_rejected.txt'.format(filename), 'w', encoding='utf-8', errors='replace')
            for f in [file_accept, file_reject]:
                f.write('Name\tOriginal ISBN\tEquivalent ISBN\tProprietary identifier\tVIAF\tISNI\tNACO\tOther identifiers\tVariant name forms\n')
                # f.write('Name\tISBN\tProprietary identifier\tVIAF\tISNI\tNACO\tOther identifiers\tVariant name forms\n')

            '''
            self.cursor.execute("""SELECT ttable.string, ttable.isbn, ttable.identifier, VIAF_isbn.VIAF, GROUP_CONCAT(VIAF_equivalences.identifier, '|'), GROUP_CONCAT(VIAF_string.string, '|')
            FROM ttable 
            INNER JOIN VIAF_isbn on ttable.isbn = VIAF_isbn.isbn
            INNER JOIN VIAF_equivalences on VIAF_isbn.VIAF = VIAF_equivalences.VIAF
            INNER JOIN VIAF_string on VIAF_isbn.VIAF = VIAF_string.VIAF
            GROUP BY VIAF_equivalences.VIAF
            ORDER BY ttable.string ASC, ttable.isbn ASC ;""")
            '''

            self.cursor.execute("""SELECT ttable.string, ttable.isbn, isbn_equivalents.isbnb, ttable.identifier, VIAF_isbn.VIAF, GROUP_CONCAT(VIAF_equivalences.identifier, '|'), GROUP_CONCAT(VIAF_string.string, '|')
            FROM ttable 
            INNER JOIN isbn_equivalents on ttable.isbn = isbn_equivalents.isbna 
            INNER JOIN VIAF_isbn on isbn_equivalents.isbnb = VIAF_isbn.isbn
            INNER JOIN VIAF_equivalences on VIAF_isbn.VIAF = VIAF_equivalences.VIAF
            INNER JOIN VIAF_string on VIAF_isbn.VIAF = VIAF_string.VIAF
            GROUP BY VIAF_equivalences.VIAF
            ORDER BY ttable.string ASC, ttable.isbn ASC ;""")



            record_count = 0
            try:
                row = list(self.cursor.fetchone())
            except:
                row = None
            while row:
                record_count += 1
                if record_count % 100 == 0:
                    print('\r{} records processed'.format(str(record_count)), end='\r')
                # string_name, isbn, identifier, viaf, other, string_name_list = row[0], row[1], row[2], row[3], row[4], row[5]
                string_name, isbn, isbnb, identifier, viaf, other, string_name_list = row[0], row[1], row[2], row[3], row[4], row[5], row[6]
                isni = '|'.join(sorted(set(o for o in other.split('|') if o.startswith('isni:'))))
                naco = '|'.join(sorted(set(o for o in other.split('|') if o.startswith('naco:'))))
                other = '|'.join(sorted(set(o for o in other.split('|') if not(o.startswith('naco:') or o.startswith('isni:')))))
                string_name_list = '|'.join(sorted(set(string_name_list.split('|'))))
                score = max(fuzz.token_set_ratio(string_name, s) for s in string_name_list.split('|'))
                f = file_accept if score >= 80 else file_reject
                # f.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(string_name, isbn, identifier, viaf, isni, naco, other, string_name_list))
                f.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(string_name, isbn, isbnb, identifier, viaf, isni, naco, other, string_name_list))
                try: row = list(self.cursor.fetchone())
                except: break
            for f in [file_accept, file_reject]:
                f.close()

            file.close()

    def write_naco_isni_equivalents(self):
        """Function to write a list of NACO and ISNI equivalent identifiers"""
        print('\nWriting NACO and ISNI equivalents ...')
        print('----------------------------------------')
        print(str(datetime.datetime.now()))

        file = open('naco_isni_equivalents.txt', 'w', encoding='utf-8', errors='replace')
        file.write('NACO ID\tISNI\n')
        record_count = 0

        for query in ["""SELECT other_equivalences.other, GROUP_CONCAT(other_equivalences.identifier, ';')
        FROM other_equivalences 
        WHERE other_equivalences.other LIKE 'naco%' AND other_equivalences.identifier LIKE 'isni%' 
        GROUP BY other_equivalences.other 
        ORDER BY other_equivalences.other ASC;""",
                      """SELECT t1.identifier, GROUP_CONCAT(t2.identifier, ';')
        FROM VIAF_equivalences as t1
        INNER JOIN VIAF_equivalences as t2
        ON t1.VIAF = t2.VIAF
        WHERE t1.identifier LIKE 'naco%' AND t2.identifier LIKE 'isni%' 
        GROUP BY t1.identifier 
        ORDER BY t1.identifier ASC;"""]:
            self.cursor.execute(query)
            try: row = list(self.cursor.fetchone())
            except: row = None
            while row:
                record_count += 1
                if record_count % 100 == 0:
                    print('\r{} records processed'.format(str(record_count)), end='\r')
                naco, isni = row[0], row[1]
                file.write('{}\t{}\n'.format(naco, isni))
                try: row = list(self.cursor.fetchone())
                except: break
            print('\r{} records processed'.format(str(record_count)), end='\r')

        file.close()
        gc.collect()
        print('{} NACO and ISNI equivalents found'.format(str(record_count)))
        return record_count

    def write_proprietary_identifiers(self):
        print('\nReporting proprietary identifiers ...')
        print('----------------------------------------')
        print(str(datetime.datetime.now()))

        files = {}
        for identifier_type in ['HarperCollins', 'Penguin', 'RandomHouse']:
            files[identifier_type] = open('{}_identifiers.txt'.format(identifier_type), 'w', encoding='utf-8', errors='replace')
            files[identifier_type].write('{} identifier\tVIAF\tISNI\tNACO\tOther identifiers\tNACO authorised name\n'.format(identifier_type))

        self.cursor.execute("""SELECT t1.identifier, GROUP_CONCAT(t2.VIAF, ';'), GROUP_CONCAT(t2.identifier, ';'), GROUP_CONCAT(NACO_authorised.string, ';')
        FROM VIAF_equivalences AS t1 
        INNER JOIN VIAF_equivalences AS t2 ON t1.VIAF = t2.VIAF 
        LEFT JOIN NACO_authorised ON NACO_authorised.NACO = SUBSTR(t2.identifier,6) 
        WHERE t1.identifier NOT LIKE 'naco%' AND  t1.identifier NOT LIKE 'isni%' AND t1.identifier NOT LIKE t2.identifier 
        GROUP BY t1.identifier 
        ORDER BY t1.identifier ASC ;""")

        record_count = 0
        try: row = list(self.cursor.fetchone())
        except: row = None
        while row:
            record_count += 1
            if record_count % 100 == 0:
                print('\r{} records processed'.format(str(record_count)), end='\r')
            identifier, viaf, equivalent, string = row[0], row[1], row[2], row[3]
            viaf = ';'.join(sorted(set(viaf.split(';'))))
            isni, naco, other = set(), set(), set()
            for e in equivalent.split(';'):
                if e.startswith('isni:'): isni.add(e)
                elif e.startswith('naco:'): naco.add(e)
                else: other.add(e)
            identifier_type = 'HarperCollins' if  identifier.startswith('harpercollins:') else 'Penguin' if  identifier.startswith('penguin:') else 'RandomHouse' if  identifier.startswith('randomhouse:') else None
            if identifier_type:
                files[identifier_type].write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(identifier, viaf, ';'.join(sorted(isni)), ';'.join(sorted(naco)), ';'.join(sorted(other)), string))
            try: row = list(self.cursor.fetchone())
            except: break

        for identifier_type in files:
            files[identifier_type].close()


# ====================
#  Control functions
# ====================


def parse_marc(record_type='BNB') -> None:
    db = IdentityGraphDatabase()
    db.add_marc(record_type=record_type)
    db.dump_database()
    db.close()


def parse_tsv() -> None:
    db = IdentityGraphDatabase()
    db.add_tsv()
    db.dump_database()
    db.close()


def parse_viaf() -> None:
    db = IdentityGraphDatabase()
    db.add_viaf_links()
    db.dump_database()
    db.close()


def parse_isbns() -> None:
    db = IdentityGraphDatabase()
    db.add_isbns()
    db.dump_database()
    db.close()


def find_name_matches() -> None:
    db = IdentityGraphDatabase()
    db.find_name_matches()
    db.close()


def index() -> None:
    db = IdentityGraphDatabase()
    db.build_indexes()
    db.close()


def export_graph() -> None:
    db = IdentityGraphDatabase()
    db.clean()
    db.dump_database()
    db.write_naco_isni_equivalents()
    db.write_proprietary_identifiers()
    db.close()


# ====================
#   General functions
# ====================


def is_null(var) -> bool:
    """Function to test whether a variable is null"""
    if var is None or not var: return True
    if isinstance(var, (str, list, tuple, set)) and len(var) == 0: return True
    if isinstance(var, str) and var == '': return True
    if isinstance(var, (int, float, complex, bool)) and int(var) == 0: return True
    return False


def clean_identifier(s, type=None):
    if s is None or not s: return None
    s = s.strip().rstrip('/').strip()
    s = re.sub(r'https?:\/\/(www\.)?(isni|viaf)\.org\/(isni|viaf)\/?', '', s).strip()
    if '/' in s:
        s = s.rsplit('/')[-1]
    if type == 'naco':
        s = re.sub(r'\s+', '', s.lower().strip().replace(' ', ''))
        if re.search(r'[^0-9nbors]]', s): return None
        if not s.startswith('n'): return None
    if type == 'isni':
        s = s.upper().strip().replace(' ', '').replace('-', '')
        if re.search(r'[^0-9X]]', s): return None
    if type == 'viaf':
        s = re.sub(r'\s*\(Personal\)', '', s).strip()
        if re.search(r'[^0-9]]', s): return None
    return s.strip()


def which(s, l):
    """Function to determine which member of a list is a substring of a given string
    (returns the first list item with this property,
    so not useful if the string contains more than one list item)"""
    for i in l:
        if i in s: return i
    return None


def date_time_message(message=None):
    if message:
        print('\n\n{} ...'.format(message))
    print('----------------------------------------')
    print(str(datetime.datetime.now()))


def date_time_exit():
    date_time_message(message='All processing complete')
    sys.exit()


def message(s) -> str:
    """Function to convert OPTIONS description to present tense"""
    if s == 'Exit program': return 'Shutting down'
    return s.replace('Parse', 'Parsing').replace('eXport', 'Exporting').replace('Find', 'Finding').replace('build', 'Building').replace('Index', 'index')


def exit_prompt(message=None):
    """Function to exit the program after prompting the use to press Enter"""
    if message: print(str(message))
    input('\nPress [Enter] to exit...')
    sys.exit()




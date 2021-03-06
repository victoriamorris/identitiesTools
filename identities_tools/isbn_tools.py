#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================
#       Set-up
# ====================

# Import required modules
import datetime
import os
import gc
import glob
import re
from identities_tools.graph_tools import *


__author__ = 'Victoria Morris'
__license__ = 'MIT License'
__version__ = '1.0.0'
__status__ = '4 - Beta Development'


# ====================
#  Regular expressions
# ====================


RE_ISBN10 = re.compile(r'ISBN\x20(?=.{13}$)\d{1,5}([- ])\d{1,7}'r'\1\d{1,6}\1(\d|X)$|[- 0-9X]{10,16}')
RE_ISBN13 = re.compile(r'97[89]{1}(?:-?\d){10,16}|97[89]{1}[- 0-9]{10,16}')


# ====================
#     Constants
# ====================


ISBN_FILE_PATH = 'K:\\Users\\Victoria\\Projects\\2019\\2019-01 - ISNI\\Data\\Publisher files\\Hachette-data.xlsx'


# ====================
#       Classes
# ====================


class Isbn(object):

    def __init__(self, content):
        self.isbn = re.sub(r'[^0-9X]', '', content.upper())
        if is_isbn_10(self.isbn):
            self.isbn = isbn_convert(self.isbn)

    def __str__(self):
        return self.isbn


# ====================
#      Functions
# ====================


def isbn_10_check_digit(nine_digits):
    """Function to get the check digit for a 10-digit ISBN"""
    if len(nine_digits) != 9: return None
    try: int(nine_digits)
    except: return None
    remainder = int(sum((i + 2) * int(x) for i, x in enumerate(reversed(nine_digits))) % 11)
    if remainder == 0: tenth_digit = 0
    else: tenth_digit = 11 - remainder
    if tenth_digit == 10: tenth_digit = 'X'
    return str(tenth_digit)


def isbn_13_check_digit(twelve_digits):
    """Function to get the check digit for a 13-digit ISBN"""
    if len(twelve_digits) != 12: return None
    try: int(twelve_digits)
    except: return None
    thirteenth_digit = 10 - int(sum((i % 2 * 2 + 1) * int(x) for i, x in enumerate(twelve_digits)) % 10)
    if thirteenth_digit == 10: thirteenth_digit = '0'
    return str(thirteenth_digit)


def isbn_10_check_structure(isbn10):
    """Function to check the structure of a 10-digit ISBN"""
    return True if re.match(RE_ISBN10, isbn10) else False


def isbn_13_check_structure(isbn13):
    """Function to check the structure of a 13-digit ISBN"""
    return True if re.match(RE_ISBN13, isbn13) else False


def is_isbn_10(isbn10):
    """Function to validate a 10-digit ISBN"""
    isbn10 = re.sub(r'[^0-9X]', '', isbn10.replace('x', 'X'))
    if len(isbn10) != 10: return False
    return False if isbn_10_check_digit(isbn10[:-1]) != isbn10[-1] else True


def is_isbn_13(isbn13):
    """Function to validate a 13-digit ISBN"""
    isbn13 = re.sub(r'[^0-9X]', '', isbn13.replace('x', 'X'))
    if len(isbn13) != 13: return False
    if isbn13[0:3] not in ('978', '979'): return False
    return False if isbn_13_check_digit(isbn13[:-1]) != isbn13[-1] else True


def isbn_convert(isbn10):
    """Function to convert a 10-digit ISBN to a 13-digit ISBN"""
    if not is_isbn_10(isbn10): return None
    return '978' + isbn10[:-1] + isbn_13_check_digit('978' + isbn10[:-1])

'''
def parse_isbn_list():

    print('\n\nParsing ISBNs from file {} ...'.format(str(ISBN_FILE_PATH)))
    print('----------------------------------------')
    print(str(datetime.datetime.now()))

    db = IdentityDatabase()
    record_count = 0
    query = 'INSERT OR IGNORE INTO isbn_string (isbn, string_name) VALUES (?, ?);'
    values = []

    file = open(ISBN_FILE_PATH, mode='r', encoding='utf-8', errors='replace')
    for filelineno, line in enumerate(file):
        record_count += 1

        if '@' in line or '|' not in line: continue
        previous_viaf = viaf
        viaf, other = line.strip().split('\t')
        viaf = clean_viaf(viaf.replace('http://viaf.org/viaf/', ''))
        if viaf != previous_viaf and previous_viaf in V:
            vals = (previous_viaf,)
            for i in sorted(AUTHORITY_TYPES):
                vals += (V[previous_viaf][i],)
            values.append(vals)
        if viaf not in V: V[viaf] = {i: None for i in sorted(AUTHORITY_TYPES)}
        identity_type, identity = other.split('|')
        if identity_type in AUTHORITY_TYPES:
            V[viaf][identity_type] = identity

        if record_count % 1000 == 0:
            print('\r{} records processed'.format(str(record_count)), end='\r')
        if len(values) >= 1000 and viaf != previous_viaf:
            db.cursor.executemany(query, values)
            db.conn.commit()
            values = []
            V = {}
            previous_viaf, viaf = None, None
            gc.collect()

    print('\r{} records processed'.format(str(record_count)), end='\r')
    if values:
        db.cursor.executemany(query, values)
        db.conn.commit()
    file.close()
    for var in [values, V, viaf, previous_viaf, query]: del var
    db.close()
'''

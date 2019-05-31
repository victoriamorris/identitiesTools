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
from identities_tools.isbn_tools import *


__author__ = 'Victoria Morris'
__license__ = 'MIT License'
__version__ = '1.0.0'
__status__ = '4 - Beta Development'


# ====================
#     Constants
# ====================

LEADER_LENGTH, DIRECTORY_ENTRY_LENGTH = 24, 12
SUBFIELD_INDICATOR, END_OF_FIELD, END_OF_RECORD = chr(0x1F), chr(0x1E), chr(0x1D)
ALEPH_CONTROL_FIELDS = ['DB ', 'FMT', 'SYS']

NODE_TYPES = ['string', 'isbn', 'isni', 'viaf', 'naco', 'harpercollins', 'penguin', 'randomhouse']

# ====================
#     Exceptions
# ====================


class RecordLengthError(Exception):
    def __str__(self): return 'Invalid record length in first 5 bytes of record'


class LeaderError(Exception):
    def __str__(self): return 'Error reading record leader'


class DirectoryError(Exception):
    def __str__(self): return 'Record directory is invalid'


class FieldsError(Exception):
    def __str__(self): return 'Error locating fields in record'


class BaseAddressLengthError(Exception):
    def __str__(self): return 'Base address exceeds size of record'


class BaseAddressError(Exception):
    def __str__(self): return 'Error locating base address of record'


# ====================
#       Classes
# ====================


class MARCReader(object):

    def __init__(self, marc_target):
        super(MARCReader, self).__init__()
        if hasattr(marc_target, 'read') and callable(marc_target.read):
            self.file_handle = marc_target

    def __iter__(self):
        return self

    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

    def __next__(self):
        first5 = self.file_handle.read(5)
        if not first5: raise StopIteration
        if len(first5) < 5: raise RecordLengthError
        return Record(first5 + self.file_handle.read(int(first5) - 5))


class Record(object):
    def __init__(self, data='', leader=' ' * LEADER_LENGTH):
        self.leader = '{}22{}4500'.format(leader[0:10], leader[12:20])
        self.fields = list()
        self.pos = 0
        if len(data) > 0: self.decode_marc(data)

    def __getitem__(self, tag):
        fields = self.get_fields(tag)
        if len(fields) > 0: return fields[0]
        return None

    def __contains__(self, tag):
        fields = self.get_fields(tag)
        return len(fields) > 0

    def __iter__(self):
        self.__pos = 0
        return self

    def __next__(self):
        if self.__pos >= len(self.fields): raise StopIteration
        self.__pos += 1
        return self.fields[self.__pos - 1]

    def __str__(self):
        text_list = ['=LDR  {}'.format(self.leader)]
        text_list.extend([str(field) for field in self.fields])
        return '\n'.join(text_list) + '\n'

    def get_fields(self, *args):
        if len(args) == 0: return self.fields
        return [f for f in self.fields if f.tag in args]

    def add_field(self, *fields):
        self.fields.extend(fields)

    def decode_marc(self, marc):
        # Extract record leader
        try:
            self.leader = marc[0:LEADER_LENGTH].decode('ascii')
        except:
            print('Record has problem with Leader and cannot be processed')
        if len(self.leader) != LEADER_LENGTH: raise LeaderError

        # Extract the byte offset where the record data starts
        base_address = int(marc[12:17])
        if base_address <= 0: raise BaseAddressError
        if base_address >= len(marc): raise BaseAddressLengthError

        # Extract directory
        # base_address-1 is used since the directory ends with an END_OF_FIELD byte
        directory = marc[LEADER_LENGTH:base_address - 1].decode('ascii')

        # Determine the number of fields in record
        if len(directory) % DIRECTORY_ENTRY_LENGTH != 0:
            raise DirectoryError
        field_total = len(directory) / DIRECTORY_ENTRY_LENGTH

        # Add fields to record using directory offsets
        field_count = 0
        while field_count < field_total:
            entry_start = field_count * DIRECTORY_ENTRY_LENGTH
            entry_end = entry_start + DIRECTORY_ENTRY_LENGTH
            entry = directory[entry_start:entry_end]
            entry_tag = entry[0:3]
            entry_length = int(entry[3:7])
            entry_offset = int(entry[7:12])
            entry_data = marc[base_address + entry_offset:base_address + entry_offset + entry_length - 1]

            # Check if tag is a control field
            if str(entry_tag) < '010' and entry_tag.isdigit():
                field = Field(tag=entry_tag, data=entry_data.decode('utf-8'))
            elif str(entry_tag) in ALEPH_CONTROL_FIELDS:
                field = Field(tag=entry_tag, data=entry_data.decode('utf-8'))

            else:
                subfields = list()
                subs = entry_data.split(SUBFIELD_INDICATOR.encode('ascii'))
                # Missing indicators are recorded as blank spaces.
                # Extra indicators are ignored.

                subs[0] = subs[0].decode('ascii') + '  '
                first_indicator, second_indicator = subs[0][0], subs[0][1]

                for subfield in subs[1:]:
                    if len(subfield) == 0: continue
                    try:
                        code, data = subfield[0:1].decode('ascii'), subfield[1:].decode('utf-8', 'strict')
                    except: pass
                        # print('Error in subfield code')
                    else:
                        subfields.append(code)
                        subfields.append(data)
                field = Field(
                    tag=entry_tag,
                    indicators=[first_indicator, second_indicator],
                    subfields=subfields,
                )
            self.add_field(field)
            field_count += 1

        if field_count == 0: raise FieldsError

    def as_marc(self):
        fields, directory = b'', b''
        offset = 0

        for field in self.fields:
            field_data = field.as_marc()
            fields += field_data
            if field.tag.isdigit():
                directory += ('%03d' % int(field.tag)).encode('utf-8')
            else:
                directory += ('%03s' % field.tag).encode('utf-8')
            directory += ('%04d%05d' % (len(field_data), offset)).encode('utf-8')
            offset += len(field_data)

        directory += END_OF_FIELD.encode('utf-8')
        fields += END_OF_RECORD.encode('utf-8')
        base_address = LEADER_LENGTH + len(directory)
        record_length = base_address + len(fields)
        strleader = '%05d%s%05d%s' % (record_length, self.leader[5:12], base_address, self.leader[17:])
        leader = strleader.encode('utf-8')
        return leader + directory + fields

    def get_name_strings(self):
        if any(s in self.fields for s in ['130', '147', '148', '150', '151', '155', '162',
                                          '180', '181', '182', '185', '240']): return set()
        for field in self.get_fields('336'):
            if 'a' in field and any(s in field['a'] for s in ['txt', 'text']): return set()
        names = set()
        for field in self.get_fields('100', '400', '700'):
            if any(s in field for s in ['f', 'h', 'k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't', 'v']): return set()
            name = field.text(subfields='abcdg').strip()
            if name != '': names.add(name)
        for field in self.get_fields('378'):
            name = field.text(subfields='q').strip()
            if name != '': names.add(name)
        return names

    def get_authorised_name(self):
        if any(s in self.fields for s in ['130', '147', '148', '150', '151', '155', '162',
                                          '180', '181', '182', '185', '240']): return None
        for field in self.get_fields('336'):
            if 'a' in field and any(s in field['a'] for s in ['txt', 'text']): return None
        for field in self.get_fields('100'):
            if any(s in field for s in ['f', 'h', 'k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't', 'v']): return None
            name = field.text(subfields='abcdg').strip()
            if name != '': return name
        return None

    def get_isbns(self, record_type='BNB'):
        isbns = set()
        for field in self.get_fields('901' if record_type == 'VIAF' else '020'):
            try: isbn = str(Isbn(field['a']))
            except: isbn = None
            if isbn: isbns.add(isbn)
        return isbns

    def get_identifiers(self, record_type='BNB'):
        identifiers = {a: set() for a in NODE_TYPES}
        identifiers['isbn'] = self.get_isbns(record_type=record_type)
        identifiers['string'] = self.get_name_strings()
        if record_type=='NACO':
            for field in self.get_fields('001'):
                identifiers['naco'].add(field.data.strip())
        for field in self.get_fields('024'):
            for subfield in field.get_subfields('a'):
                if 'viaf' in subfield or ('2' in field and 'viaf' in field['2']):
                    identifiers['viaf'].add(clean_identifier(subfield.strip(), type='viaf'))
                elif 'isni' in subfield or ('2' in field and 'isni' in field['2']):
                    identifiers['isni'].add(clean_identifier(subfield.strip(), type='isni'))
        for field in self.get_fields('100', '400', '600', '700'):
            if any(s in field for s in ['f', 'h', 'k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't', 'v']): continue
            for subfield in field.get_subfields('0'):
                if subfield.startswith('(ISNI)'):
                    identifiers['isni'].add(clean_identifier(subfield.strip().replace('(ISNI)', ''), type='isni'))
                elif subfield.startswith('(VIAF)'):
                    identifiers['viaf'].add(clean_identifier(subfield.strip().replace('(VIAF)', ''), type='viaf'))
                elif subfield.startswith('(LC)'):
                    identifiers['naco'].add(clean_identifier(subfield.strip().replace('(LC)', ''), type='naco'))
            if record_type == 'BNB':
                for subfield in field.get_subfields('8'):
                    if 'isni' in subfield:
                        identifiers['isni'].add(clean_identifier(subfield.strip(), type='isni'))
                for subfield in field.get_subfields('9'):
                    if 'viaf' in subfield:
                        identifiers['viaf'].add(clean_identifier(subfield.strip(), type='viaf'))
        return identifiers


class Field(object):

    def __init__(self, tag, indicators=None, subfields=None, data=''):
        if indicators is None: indicators = []
        if subfields is None: subfields = []
        indicators = [str(x) for x in indicators]

        # Normalize tag to three digits
        self.tag = '%03s' % tag

        # Check if tag is a control field
        if self.tag < '010' and self.tag.isdigit():
            self.data = str(data)
        elif self.tag in ALEPH_CONTROL_FIELDS:
            self.data = str(data)
        else:
            self.indicator1, self.indicator2 = self.indicators = indicators
            self.subfields = subfields

    def __iter__(self):
        self.__pos = 0
        return self

    def __getitem__(self, subfield):
        subfields = self.get_subfields(subfield)
        if len(subfields) > 0: return subfields[0]
        return None

    def __contains__(self, subfield):
        subfields = self.get_subfields(subfield)
        return len(subfields) > 0

    def __next__(self):
        if not hasattr(self, 'subfields'):
            raise StopIteration
        while self.__pos + 1 < len(self.subfields):
            subfield = (self.subfields[self.__pos], self.subfields[self.__pos + 1])
            self.__pos += 2
            return subfield
        raise StopIteration

    def __str__(self):
        if self.is_control_field() or self.tag in ALEPH_CONTROL_FIELDS:
            return '={}  {}'.format(self.tag, self.data.replace(' ', '#'))
        text = '={}  '.format(self.tag)
        for indicator in self.indicators:
            if indicator in (' ', '#'):
                text += '#'
            else:
                text += indicator
        text += ' '
        for subfield in self:
            text += '${}{}'.format(subfield[0], subfield[1])
        return text

    def text(self, subfields=''):
        if self.is_control_field() or self.tag in ALEPH_CONTROL_FIELDS:
            return self.data.replace(' ', '#')
        return ' '.join(subfield[1] for subfield in self if subfield[0] in subfields)

    def get_subfields(self, *codes):
        values = []
        for subfield in self:
            if len(codes) == 0 or subfield[0] in codes:
                values.append(str(subfield[1]))
        return values

    def is_control_field(self):
        if self.tag < '010' and self.tag.isdigit(): return True
        if self.tag in ALEPH_CONTROL_FIELDS: return True
        return False

    def as_marc(self):
        if self.is_control_field():
            return (self.data + END_OF_FIELD).encode('utf-8')
        marc = self.indicator1 + self.indicator2
        for subfield in self:
            marc += SUBFIELD_INDICATOR + subfield[0] + subfield[1]
        return (marc + END_OF_FIELD).encode('utf-8')


# ====================
#      Functions
# ====================


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


'''
def parse_marc_file(record_type='BNB'):

    # This currently only works for NACO records, which do not contain ISBNs

    #db = IdentityDatabase()
    db = IdentityGraphDatabase()

    queries, values = {}, {}
    for table in GRAPH_TABLES:
        queries['{}'.format(table)] = 'INSERT OR IGNORE INTO {} ({}) VALUES (?, ?);'.format(table, ', '.join(key for (key, value) in GRAPH_TABLES[table]))
        values['{}'.format(table)] = []

    if record_type == 'NACO': file_list = glob.glob('\\'.join((NACO_FILE_PATH, NACO_FILE_PATTERN)))
    else: file_list = glob.glob('\\'.join((BNB_FILE_PATH, BNB_FILE_PATTERN)))
    for f in file_list:

        print('\n\nParsing {} file {} ...'.format(record_type, str(f)))
        print('----------------------------------------')
        print(str(datetime.datetime.now()))

        record_count = 0
        file = open(f, mode='rb')
        reader = MARCReader(file)
        for record in reader:
            record_count += 1
            identifiers = record.get_identifiers(record_type=record_type)
            names = record.get_name_strings()
            authorised_name = record.get_authorised_name()
            if not authorised_name: continue

            for n in identifiers['NACO']:
                values['NACO_authorised'].append((n, authorised_name))
                for name in names:
                    if name == authorised_name: continue
                    values['NACO_variants'].append((n, name))

            for name in names:
                for isbn in identifiers['isbn']:
                    values['string_isbn'].append((name, isbn))

            if len(identifiers['VIAF']) > 0:
                for v in identifiers['VIAF']:
                    for n in identifiers['NACO']:
                        values['VIAF_equivalences'].append(('viaf:{}'.format(v), 'naco:{}'.format(n)))
                    for i in identifiers['ISNI']:
                        values['VIAF_equivalences'].append(('viaf:{}'.format(v), 'isni:{}'.format(i)))
                    for isbn in identifiers['isbn']:
                        values['VIAF_isbn'].append(('viaf:{}'.format(v), isbn))
            else:
                for n in identifiers['NACO']:
                    for i in identifiers['ISNI']:
                        values['other_equivalences'].append(('naco:{}'.format(n), 'isni:{}'.format(i)))
                        for isbn in identifiers['isbn']:
                            values['other_isbn'].append(('isni:{}'.format(i), isbn))
                    for isbn in identifiers['isbn']:
                        values['other_isbn'].append(('naco:{}'.format(n), isbn))

            if record_count % 10000 == 0:
                print('\r{} records processed'.format(str(record_count)), end='\r')
                for v in queries:
                    values[v] = db.execute_all(queries[v], values[v])



        file.close()
        print('\r{} records processed'.format(str(record_count)), end='\r')
        for v in queries:
            db.execute_all(queries[v], values[v])

        db.clean()

    db.dump_database()
    db.close()
'''

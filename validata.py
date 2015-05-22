#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Validata gives you data valid
v1.0.0

Validata is designed for line-based data pipeline or general purpose data validation.
For more information please contact kaedetai@gmail.com.

Usage:

python validata.py config.yaml datafile.ext [...]

  or

from validata import *
validata = Validata('config.yaml')
validata.check_file(filename)
"""

#define constants
__VALIDATA_ROOT__ = '/usr/local/validata'
__VALIDATA_ETC__ = __VALIDATA_ROOT__ + '/etc'
__HISTORY_LOG__ = 'http://54.186.241.214/history.html'
__HISTORY_API__ = 'http://54.186.241.214/cgi-bin/api.py'

#import libraries
import re
import yaml
from sys import exit, argv
from os.path import isfile, dirname, abspath
from datetime import datetime
from urllib import urlencode
from urllib2 import urlopen

#define errors
class FileNotFoundError(Exception):
    ""
    def __init__(self, filename, pathlist):
        self.filename = filename
        self.pathlist = pathlist
    def __str__(self):
        return 'Error: Unable to read file "%s" in path "%s"!' % (self.filename, ':'.join(self.pathlist))

class ConfigError(Exception):
    ""
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return 'Error: %s' % self.msg

class KeyNotFoundError(Exception):
    def __init__(self, key):
        self.key = key
    def __str__(self):
        return 'Error: Undefined rule "%s" has been used!' % self.key

class InvalidValueError(Exception):
    def __init__(self, key, value):
        self.key = key
        self.value = value.encode('utf8')
    def __str__(self):
        return 'Error: Group "%s" has invalid value "%s"!' % (self.key, self.value)

class PatternNotMatchError(Exception):
    def __init__(self, pattern, line):
        self.pattern = pattern
        self.line = line.encode('utf8')
    def __str__(self):
        return 'Error: "%s" does not match pattern "%s"!' % (self.line, self.pattern)

class Rule:
    """Base Rule class that used by Validata object. DO NOT USE IT DIRECTLY!
    Args:
        pattern (str): The pattern to be compiled
        validata (Validata): The parrent Validata object
    """
    def __init__(self, pattern, validata):
        self.pattern = pattern
        self.validata = validata
        try:
            self.rule = re.compile(pattern)
        except:
            raise ConfigError('Invalid pattern "%s"!' % pattern)
    def __repr__(self):
        return self.pattern
    def validate(line):
        return True

class AndRule(Rule):
    """A list of rules that all of them need to be matched"""
    def __init__(self, rules):
        self.rules = rules
        self.pattern = ' && '.join(r.__repr__() for r in rules)
    def __repr__(self):
        return self.pattern
    def validate(self, line):
        return all([rule.validate(line) for rule in self.rules])

class SearchRule(Rule):
    """Rule that do re.search"""
    def validate(self, line):
        found = self.rule.search(line)
        if not found or not self.validata.check_group(found):
            raise PatternNotMatchError(self.pattern, line)
        return True

class FindIterRule(Rule):
    """Rule that do re.finditer"""
    def validate(self, line):
        return all(self.validata.check_group(found) for found in self.rule.finditer(line))

class Validata:
    """Validate loads config from a yaml file and compile them into Rule objects.
    Args:
        filename (str): The config file name
    Raises:
        ConfigError: Something wrong in the config file that must be fixed.
    """
    def __init__(self, filename):
        self.version = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.logfile = None
        self.include = set()
        self.range = None
        self.rules = rules = {}
        self.config = cfg = self.load_config(filename)
        #compile config into rules
        for key in cfg:
            if key in rules:
                continue
            #ignore keywords
            if key[:2] == '__':
                continue
            #constants
            if key[:1] == '_':
                if isinstance(cfg[key], list):
                    rules[key] = set(cfg[key])
                else:
                    pass #todo
                continue
            #prevent to reference rules recursively
            self.ref = set([key])
            #compile regular expression
            rules[key] = self.compile_rule(cfg[key])
        if 'all' not in rules:
            raise ConfigError('Rule "all" must be defined!')

    def __repr__(self):
        return '\n'.join(sorted(k + ': ' + self.rules[k].__repr__() for k in self.rules))

    def find_file(self, filename, pathlist = ['.']):
        """Search in the path_list to find the file or open from URL.
        Args:
            filename (str): Filename in relative or absolute path or URL.
            pathlist (str): A list of path to search.
        Returns:
            (file, str): File object and the absolute path of the file.
        """
        if filename.startswith('http://') or filename.startswith('https://'):
            return (urlopen(filename), filename)
        for path in pathlist:
            filepath = abspath(path + '/' + filename)
            if isfile(filepath):
                f = open(filepath, 'r')
                return (f, filepath)
        raise FileNotFoundError(filename, pathlist)

    def load_config(self, filename, basedir = '.'):
        """Load config file recursively.
        Args:
            filename (str): Config filename to.
            basedir (str): Base directory to find the include file.
        Returns:
            dict: Key value mapped config.
        """
        #find absolute path for config file
        (f, filepath) = self.find_file(filename, [basedir, __VALIDATA_ETC__])
        if filepath in self.include:
            raise ConfigError('Recursively include config file "%s"!' % filepath)
        self.include.add(filepath)
        cfg = yaml.load(f)
        f.close()
        #decide base directory for current config file
        basedir = dirname(filepath)
        #get log file path
        if '__logfile' in cfg:
            logfile = cfg['__logfile']
            if logfile[0] != '/':
                logfile = basedir + '/' + logfile
            self.logfile = logfile
        #get data range
        if '__range' in cfg:
            if isinstance(cfg['__range'], int):
                self.range = (cfg['__range'], None)
            else:
                found = re.match('\s*(-?[0-9]+)\s*,\s*(-?[0-9]+)\s*', cfg['__range'])
                if not found:
                    raise ConfigError('Failed to parse range "%s"!' % cfg['__range'])
                self.range = tuple(map(int, found.groups()))
        #check if there's any external reference
        for key in cfg:
            #ignore keywords
            if key[:2] == '__':
                continue
            elif key[:1] == '_':
                if isinstance(cfg[key], list):
                    continue
                #load external reference
                refname = cfg[key]
                (f, filepath) = self.find_file(refname, [basedir, __VALIDATA_ETC__])
                cfg[key] = [x.rstrip('\r\n').decode('utf8') for x in f if x.rstrip('\r\n') != '']
                f.close()
                print 'Reference file "%s" loaded.' % refname
        #load include file(s)
        if '__include' in cfg:
            include = cfg['__include']
            del cfg['__include']
            if not isinstance(include, list):
                include = [include]
            tmp = {}
            for i in include:
                tmp.update(self.load_config(i, basedir))
            tmp.update(cfg)
            cfg = tmp
        print 'Config file "%s" loaded.' % filename
        return cfg

    def compile_rule(self, pattern):
        """Compile regular expression rule(s).
        Args:
            pattern (str, list): Regular expression pattern(s) or reference(s).
        Returns:
            Rule: Compiled rule object.
        """
        cfg, rules = self.config, self.rules
        if isinstance(pattern, list):
            return AndRule(self.compile_rule(p) for p in pattern)
        if pattern[0] == '?':
            return FindIterRule(pattern[1:], self)
        if pattern[0] == '$':
            #reference
            key = pattern[1:]
            if key in rules:
                return rules[key]
            if key not in cfg:
                raise ConfigError('Reference "%s" not defined!' % pattern)
            if key in self.ref:
                raise ConfigError('Recursively reference to key "%s"' % key)
            self.ref.add(key)
            rules[key] = self.compile_rule(cfg[key])
            return rules[key]
        return SearchRule(pattern, self)

    def check_group(self, found):
        """Check if all the groups found in the given object match the rules. This is a recursive function.
        Args:
            found (_sre.SRE_Match): The match object returned by re.search().
        Returns:
            bool: True if the validation succeed.
        """
        rules = self.rules
        for key in found.groupdict():
            value = found.group(key)
            if key not in rules:
                raise KeyNotFoundError(key)
            if key[0] == '_':
                #match a predefined list
                if value not in rules[key]:
                    raise InvalidValueError(key, value)
            elif not rules[key].validate(value):
                raise InvalidValueError(key, value)
        return True

    def check_line(self, line):
        """Check if the line match the rules.
        Args:
            line (str): The line to be validated.
        Returns:
            bool: True if the validation succeed.
        Raises:
            KeyNotFoundError: A key named in the pattern has no definition.
            InvalidValueError: A value found in the pattern is not expected.
        """
        line = line.rstrip('\r\n')
        try:
            line = line.decode('utf8')
        except:
            pass
        return self.rules['all'].validate(line)

    def get_range(self, range, last):
        found = re.match('\s*([+-]?)([0-9]+)(%?)(\s*,\s*([+-]?)([0-9]+)(%?))?\s*', str(range))
        if not found:
            raise ConfigError('Failed to parse range "%s"!' % str(range))
        (s1, n1, p1, v2, s2, n2, p2) = found.groups()
        if v2:
            d1 = last * int(n1) / 100.0 if p1 else int(n1)
            d2 = last * int(n2) / 100.0 if p2 else int(n2)
            m = last + d1 if s1 == '+' else last - d1 if s1 == '-' or p1 else d1
            M = last - d2 if s2 == '-' else last + d2 if s2 == '+' or p2 else d2
        else:
            d = last * int(n1) / 100.0 if p1 else int(n1)
            m = last - d
            M = last + d
        return m, M

    def check_size(self, filename, size):
        """Check if the change of data size is in the expected range, and keep track of it.
        Args:
            filename (str): The filename to be monitored.
            size (int): The current size.
        Returns:
            bool: True if the validation succeed.
        Raises:
            ConfigError: Something wrong in the config file that must be fixed.
        """
        cfg = self.config
        if '__size' not in cfg:
            raise ConfigError('"__size" is not defined!')
        if '__logfile' not in cfg:
            raise ConfigError('"__logfile" is not defined!')
        if 'valid' not in cfg['__size']:
            raise ConfigError('"__size.valid" is not defined!')
        if isfile(self.logfile):
            try:
                with open(self.logfile, 'r') as f:
                    log = f.read()
                    log = {} if log == '' else yaml.load(log)
            except:
                raise ConfigError('Log file "%s" is corrupted!' % self.logfile)
        else:
            try:
                with open(self.logfile, 'w') as f:
                    log = {}
                    f.write('')
            except:
                raise ConfigError('Failed to create log file "%s"!' % self.logfile)
        #if this is the first time to see a file, use the current size as the last size
        last = log[filename]['last'] if filename in log and 'last' in log[filename] else size
        delta = 0
        #check if the size pass the constraint
        valid = self.get_range(cfg['__size']['valid'], last)
        alert = self.get_range(cfg['__size']['alert'], last) if 'alert' in cfg['__size'] else (0, -1)
        #print last, valid, alert
        if valid[0] <= size <= valid[1]:
            #valid
            last = size
        elif alert[0] <= size <= alert[1]:
            #alert
            if size < valid[0]:
                delta = -1
                print 'Warning: Size of "%s" decreased by %i!' % (filename, last - size)
            else:
                delta = 1
                print 'Warning: Size of "%s" increased by %i!' % (filename, last - size)
            pass #todo
        else:
            #failed
            if size < alert[0]:
                delta = -2
                print 'Error: Size of "%s" decreased by %i!' % (filename, last - size)
            else:
                delta = 2
                print 'Error: Size of "%s" increased by %i!' % (filename, last - size)
            pass #todo
        if filename not in log:
            log[filename] = {'log': []}
        log[filename]['last'] = last
        log[filename]['log'].append([self.version, size, delta])
        with open(self.logfile, 'w') as f:
            f.write(yaml.dump(log))
        return delta

    def check_file(self, filename):
        #get the data range
        with open(filename) as f:
            total = sum(1 for l in f)
        (start, stop) = (0, total)
        if self.range:
            (start, stop) = self.range
            start = start if start >= 0 else total + start
            stop = total if not stop else stop if stop > 0 else total + stop
        #validate line by line
        i = 0
        size = 0
        error = 0
        with open(filename) as f:
            for line in f:
                if i < start:
                    continue
                i += 1
                if i > stop:
                    break
                try:
                    self.check_line(line)
                except Exception as e:
                    if error < 3:
                         print 'Validation failed on file "%s",  line %i:\n%s' % (filename, i, e)
                    error += 1
                size += 1
        if error >= 3:
            print '... total errors: %i' % error
        #check data size
        try:
            delta = self.check_size(filename, size)
        except Exception as e:
            print e
            return False
        #upload result
        log = {'filename': filename, 'version': self.version, 'size': size, 'delta': delta, 'error': error}
        try:
            urlopen(__HISTORY_API__, urlencode(log)).read()
        except Exception as e:
            pass #todo
        return error == 0 and delta ** 2 <= 1

if __name__ == "__main__":
    #check parameters
    if len(argv) < 3 or not isfile(argv[1]) or argv[1][-5:] != '.yaml':
        exit('Usage:\n\npython validata.py config.yaml datafile.ext [...]')
    for i in range(2, len(argv)):
        if not isfile(argv[i]):
            exit('Error: Data File "' + argv[i] + '" not exists!')

    #load the config file
    try:
        validata = Validata(argv[1])
    except Exception as e:
        exit(e)

    #validate each data files
    failed = False
    for i in range(2, len(argv)):
        filename = argv[i]
        if validata.check_file(filename):
            print 'File "%s" is valid.' % filename
        else:
            print 'File "%s" is invalid.' % filename
            failed = True
        print 'History log is in %s?%s' % (__HISTORY_LOG__, urlencode({'filename': filename}))
    if failed:
        exit('Validation failed!')

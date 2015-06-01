#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Validata gives you data valid

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

#@debug
def debug(func):
     def inner(*args, **kwargs): #1
         print 'CALL %s\nARGS %s\nKWARGS %s' % (func.__name__, args, kwargs)
         return func(*args, **kwargs) #2
     return inner

#define errors
class FileNotFoundError(Exception):
    def __init__(self, filename, pathlist):
        self.filename = filename
        self.pathlist = pathlist
    def __str__(self):
        return 'Error: Unable to read file "%s" in path "%s"!' % (self.filename, ':'.join(self.pathlist))

class ConfigError(Exception):
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
    """Base Rule class that will always be True"""
    def __repr__(self):
        return 'True'
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
        return all(rule.validate(line) for rule in self.rules)

class AsRule(Rule):
    """Rule that do re.search
    Args:
        pattern (str): The pattern that must match
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
        return 'as: ' + self.pattern
    def validate(self, line):
        found = self.rule.search(line)
        if not found or not self.validata.check_found(found):
            raise PatternNotMatchError(self.pattern, line)
        return True

class FindRule(Rule):
    """Rule that do re.finditer
    Args:
        pattern (str): The pattern that must match
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
        return 'find: ' + self.pattern
    def validate(self, line):
        return all(self.validata.check_found(found) for found in self.rule.finditer(line))

class SplitRule(Rule):
    """Rule that do re.split
    Args:
        pattern (str): The pattern to split a line into parts
        foreach (Rule): The rule that must be matched for each part
        validata (Validata): The parrent Validata object
    """
    def __init__(self, pattern, foreach, validata):
        self.pattern = pattern
        self.foreach = foreach
        self.validata = validata
        try:
            self.rule = re.compile(pattern)
        except:
            raise ConfigError('Invalid pattern "%s"!' % pattern)
    def __repr__(self):
        return 'split: ' + self.pattern + ' => ' + self.foreach.__repr__()
    def validate(self, line):
        #skip empty line
        if line == '':
            return True
        return all(self.foreach.validate(part) for part in self.rule.split(line))

class CountRule(Rule):
    """Count the hit.
    Args:
        name (str): The name
        validata (Validata): The parrent Validata object
    """
    def __init__(self, name, validata):
        self.name = name
        self.validata = validata
    def __repr__(self):
        return 'count: ' + self.name
    def validate(self, line):
        count = self.validata.count
        if self.name not in count:
            count[self.name] = 1
        else:
            count[self.name] += 1
        return True

class GroupRule(Rule):
    """Group by value and count the size.
    Args:
        name (str): The group name
        validata (Validata): The parrent Validata object
    """
    def __init__(self, name, validata):
        self.name = name
        self.validata = validata
    def __repr__(self):
        return 'group: ' + self.name
    def validate(self, line):
        group = self.validata.group
        if self.name not in group:
            group[self.name] = {}
        me = group[self.name]
        if line in me:
            me[line] += 1
        else:
            me[line] = 1
        return True

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
        self.size = None
        self.count = {}
        self.group = {}
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

        #get data range
        if '__range' in cfg:
            if isinstance(cfg['__range'], int):
                self.range = (cfg['__range'], None)
            else:
                found = re.match('\s*(-?[0-9]+)\s*,\s*(-?[0-9]+)\s*', cfg['__range'])
                if not found:
                    raise ConfigError('Failed to parse range "%s"!' % cfg['__range'])
                self.range = tuple(map(int, found.groups()))

        #get range for size validation
        self.size = size = cfg['__size'] if '__size' in cfg and isinstance(cfg['__size'], dict) else {}
        if 'valid' not in size:
            print '"__size.valid" is not defined! Use 0 as default!'
            size['valid'] = '0'
        #verify the format
        self.get_range(size['valid'], 100)
        if 'alert' in size:
            self.get_range(size['alert'], 100)

        #load log file
        if '__logfile' not in cfg:
            print '"__logfile" is not defined! Use /tmp/validata.log as default!'
            self.logfile = '/tmp/validata.log'
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
        self.log = log

        #check if Rule "all" is defined
        if 'all' not in rules:
            raise ConfigError('Rule "all" must be defined!')

    def __repr__(self):
        return 'Validata: {\n  ' + '\n  '.join(sorted(k + ': ' + self.rules[k].__repr__() for k in self.rules)) + '\n}'

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
        for path in [''] + pathlist:
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
        Raises:
            ConfigError: Something wrong in the config file that must be fixed.
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

    def compile_rule(self, cmd):
        """Compile regular expression rule(s).
        Args:
            cmd (str, list): Command(s), regular expression pattern(s) or reference(s).
        Returns:
            Rule: Compiled rule object.
        Raises:
            ConfigError: Something wrong in the config file that must be fixed.
        """
        cfg, rules = self.config, self.rules
        if cmd == None:
            return Rule()
        if isinstance(cmd, dict):
            r = []
            if 'as' in cmd:
                r += [self.compile_rule(cmd['as'])]
            if 'find' in cmd:
                r += [FindRule(cmd['find'], self)]
            if 'split' in cmd:
                c = cmd['split']
                if 'by' not in c:
                    raise ConfigError('"split.by" is not defined!')
                if 'as' not in c:
                    raise ConfigError('"split.as" is not defined!')
                return SplitRule(c['by'], self.compile_rule(c['as']), self)
            if 'count' in cmd:
                r += [CountRule(cmd['count'], self)]
            if 'group' in cmd:
                r += [GroupRule(cmd['group'], self)]
            if len(r) == 0:
                return Rule()
            return AndRule(r) if len(r) > 1 else r[0]
        if isinstance(cmd, list):
            return AndRule([self.compile_rule(c) for c in cmd])
        if cmd[0] == '?':
            return FindRule(cmd[1:], self)
        if cmd[0] == '$':
            #reference
            key = cmd[1:]
            if key in rules:
                return rules[key]
            if key not in cfg:
                raise ConfigError('Reference "%s" not defined!' % cmd)
            if key in self.ref:
                raise ConfigError('Recursively reference to key "%s"' % key)
            self.ref.add(key)
            rules[key] = self.compile_rule(cfg[key])
            return rules[key]
        return AsRule(cmd, self)

    def check_found(self, found):
        """Check if all the groups found in the given object match the rules. This is a recursive function.
        Args:
            found (_sre.SRE_Match): The match object returned by re.search().
        Returns:
            bool: True if the validation succeed.
        Raises:
            KeyNotFoundError: A key named in the pattern has no definition.
            InvalidValueError: A value found in the pattern is not expected.
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
        """
        line = line.rstrip('\r\n')
        try:
            line = line.decode('utf8')
        except:
            pass
        return self.rules['all'].validate(line)

    def get_range(self, range, last):
        """Get the range according to the last size and the definition in __size.
        Args:
            range (str): It can be one or two constant value, difference or percentage separated by comma.
            last (int): The last size.
        Returns:
            int, int: The begin and end value of the range.
        Raises:
            ConfigError: Something wrong in the config file that must be fixed.
        """
        found = re.match('\s*([+-]?)([0-9]+)(%?)(\s*,\s*([+-]?)([0-9]+)(%?))?\s*', str(range))
        if not found:
            raise ConfigError('Failed to parse range "%s" in "__size"!' % str(range))
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

    def check_size(self, last, size):
        """Check if the change of data size is in the expected range, and keep track of it.
        Args:
            last (int): The last size.
            size (int): The current size.
        Returns:
            int: 0 for valid, -1 & 1 for alert, -2 & 2 for error.
        """
        #check if the size pass the constraint
        valid = self.get_range(self.size['valid'], last)
        alert = self.get_range(self.size['alert'], last) if 'alert' in self.size else (0, -1)
        if valid[0] <= size <= valid[1]:
            #valid
            delta = 0
        elif alert[0] <= size <= alert[1]:
            #alert
            if size < valid[0]:
                delta = -1
                print 'Warning: Size of "%s" decreased by %i!' % (filename, last - size)
            else:
                delta = 1
                print 'Warning: Size of "%s" increased by %i!' % (filename, size - last)
            pass #todo
        else:
            #failed
            if size < alert[0]:
                delta = -2
                print 'Error: Size of "%s" decreased by %i!' % (filename, last - size)
            else:
                delta = 2
                print 'Error: Size of "%s" increased by %i!' % (filename, size - last)
            pass #todo
        return delta

    def check_file(self, filename):
        #reset count and group
        self.count = {}
        self.group = {}

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
        absname = abspath(filename)
        if absname not in self.log:
            self.log[absname] = {'log': {}}
        log = self.log[absname]
        #if this is the first time to see a file, use the current size as the last size
        old = last = log['last'] if 'last' in log else size
        delta = self.check_size(last, size)
        if delta == 0:
            last = size

        #update log
        log['last'] = last
        result = {'error': error, 'last': old, 'size': size, 'delta': delta}
        if len(self.count):
            result['count'] = self.count
        if len(self.group):
            result['group'] = self.group
        log['log'][self.version] = result
        with open(self.logfile, 'w') as f:
            f.write(yaml.safe_dump(self.log, allow_unicode=True))

        #upload result
        result['filename'] = filename
        result['version'] = self.version
        try:
            urlopen(__HISTORY_API__, urlencode(result)).read()
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

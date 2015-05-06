#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Validata - the data validator
v1.1.1

Validata is designed for line-based data pipeline or general purpose data validation.
For more information please contact kaede@yahoo-inc.com or refer to:
https://docs.google.com/a/yahoo-inc.com/presentation/d/1jzfDlmuaE1J7N-jzQh1XeeGz0PCjDnvVpnRWnMqGbIo

Usage:

python validata.py config.yaml datafile.ext [...]

  or

from validata import *
validata = Validata('config.yaml')
print validata
for line in file:
    validata.check_line(line)
"""

import re
import yaml
from sys import exit, argv
from os.path import isfile, dirname, abspath

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

class Rule:
    """Base Rule class that used by Validata object. DO NOT USE IT DIRECTLY!
    Args:
        pattern (str): The pattern to be compiled
        validata (Validata): The parrent Validata object
    """
    def __init__(self, pattern, validata):
        self.pattern = pattern
        self.validata = validata
        self.rule = re.compile(pattern)
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
        return self.validata.check_group(found) if found else False

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
        self.inc = set()
        self.rules = rules = {}
        self.cfg = cfg = self.load_config(filename)
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

    def load_config(self, filename, basedir = ''):
        """Load config file recursively.
        Args:
            filename (str): Config filename to.
            basedir (str): Base directory to find the include file.
        Returns:
            dict: Key value mapped config.
        """
        if filename in self.inc:
            raise ConfigError('Recursively include config file "%s"!' % filename)
        #find absolute path for config file
        if isfile(filename):
            filename = abspath(filename)
        else:
            filename = basedir + filename
            if not isfile(filename):
                raise ConfigError('Config file "%s" is missing!' % filename)
        #decide base directory for current config file
        basedir = dirname(abspath(filename)) + '/'
        #load config file
        with open(filename, 'r') as f:
            cfg = yaml.load(f)
        #prevent to include files recursively
        self.inc.add(filename)
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
                if isfile(refname):
                    refname = abspath(refname)
                else:
                    refname = basedir + refname
                    if not isfile(refname):
                        raise ConfigError('Reference file "%s" is missing!' % refname)
                with open(refname, 'r') as f:
                    cfg[key] = [x.rstrip('\r\n').decode('utf8') for x in f if x.rstrip('\r\n') != '']
                print 'Reference file "%s" loaded.' % refname
        #load include file(s)
        if '__include' in cfg:
            inc = cfg['__include']
            del cfg['__include']
            if not isinstance(inc, list):
                inc = [inc]
            tmp = {}
            for i in inc:
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
        cfg, rules = self.cfg, self.rules
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
        """Check if the line match the rules. This is a public function.
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

    #validate data files line by line
    for i in range(2, len(argv)):
        cnt = 0
        for line in open(argv[2]):
            cnt += 1
            try:
                validata.check_line(line)
            except Exception as e:
                exit('Validation failed on file "%s",  line %i:\n%s\n%s' % (argv[i], i, line, e))
    print 'Validated.'

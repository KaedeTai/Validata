CREATE DATABASE `validata`;
USE `validata`;
CREATE TABLE IF NOT EXISTS `log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `host` varchar(255) NOT NULL,
  `filename` varchar(255) NOT NULL,
  `version` varchar(255) NOT NULL,
  `size` int(11) NOT NULL,
  `delta` int(11) NOT NULL,
  `error` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `filename` (`filename`),
  KEY `version` (`version`),
  KEY `delta` (`delta`),
  KEY `error` (`error`),
  KEY `ip` (`host`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='Validation History Log';


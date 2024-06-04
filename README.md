# CRISalid directory bridge

## Overview 

CRISalid directory bridge is a backend component of CRISalid modular CRIS (current research information system).

It is intended to watch people and organization data from various institutional sources (e.g. LDAP directory, custom HR databases, 
Excel files, laboratory apps, etc.)
and to convert it into a common format that can be fed into CRISalid institutional knowledge graph and other information system components.

The bridge is designed to be modular and extensible, so that it can be easily adapted to local data sources and formats, 
especially in the case when :
- the institutional LDAP directory is not up-to-date but other reliable sources are available
- the institutional LDAP directory is not fully [Supann](https://services.renater.fr/documentation/supann/index) compliant or conforms to an old version of the Supann schema
- part of the laboratories or institution divisions have their own directories from which data can be extracted
- some kind of structures of singular type are not referenced in the LDAP directory but are described by other sources

## Features

- Watch one or several sources of data (LDAP, HR databases, Excel files, REST APIs, etc.) and monitor changes (added/removed/updated entries)
- Identify overlaps and conflicts between sources and apply a resolution strategy (based on shared identifiers and data sources priorities)
- Convert data into a common data model approaching [CERIF-2 (Common European Research Information Format)](https://github.com/EuroCRIS/CERIF-Core) to allow integration into CRISalid knowledge graph
- Broadcast changes to other CRISalid components (e.g. CRISalid knowledge graph, etc.) through a REST API and a message broker (RabbitMQ)

## Usage example
The Xyz research institution wants to extract people and structures information from various sources :
- for the ABC laboratory, the data is available from a laboratory application REST API
- in common cases, the data is available from the institutional LDAP directory, but is overlapping and partially conflicting with the data from the ABC laboratory
- for a very particular DEF Research group, the data is only available from an Excel file that can be uploaded by the research department

In this particular case, the CRISalid directory bridge can be configured to watch the ABC laboratory application REST API, the institutional LDAP directory and the Excel file upload endpoint.
The two most reliable sources (the ABC laboratory application and the DEF Research group Excel file) can be given a higher priority than the institutional LDAP directory.
CRISalid directory bridge can be configured to prevent additional people from being affiliated to the ABC laboratory or to the DEF Research group from other data sources.
But for people already known, data from other sources like LDAP can be used to enrich existing data.
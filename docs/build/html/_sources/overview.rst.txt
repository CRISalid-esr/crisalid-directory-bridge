Overview
========

**CRISalid directory bridge** is a backend component of CRISalid modular CRIS (current research information system).

It is intended to watch people and organization data from various institutional sources (e.g. LDAP directory, custom HR
databases,
Excel files, laboratory apps, etc.)
and to convert it into a common format that can be fed into CRISalid institutional knowledge graph and other information
system components.

The bridge is designed to be modular and extensible, so that it can be easily adapted to local data sources and formats,
especially in the case when :

- the institutional LDAP directory is not up-to-date but other reliable sources are available, like the National
  Research Structure Registry (RNSR)
- the institutional LDAP directory is not fully [Supann](https://services.renater.fr/documentation/supann/index)
  compliant or conforms to an old version of the Supann schema
- part of the laboratories or institution divisions have their own directories from which data can be extracted
- some kind of structures of singular type are not referenced in the LDAP directory but are described by other sources


Project repository
------------------

.. |ico1| image:: https://img.shields.io/badge/GitHub--repository-CRISalid--directory--bridge-blue?style=flat-square&logo=GitHub
    :target: https://github.com/CRISalid-esr/crisalid-directory-bridge#readme
    :alt: GitHub repository

|ico1| Source code is available on Github

Overall operating mode
----------------------


Key features
--------------


Functional scope limitations
-----------------------------

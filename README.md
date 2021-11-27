# Collect job ads and remove PII

This is an azure function, designed to be used with Fivetran's azure function source. The function collects job ads from arbeidsplassen.no, and removes person names using SpaCy (replaced by PER, the entity label). Numbers with more than five digits (including potential spaces) are replaced with "NUM" as these are often phone numbers.

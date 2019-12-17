with CTEListNonPrimaryIndexes as (
        SELECT i.tabschema, i.tabname, i.indname
        FROM syscat.indexes AS i
        where
                i.tabschema='{DB2_SCHEMA}' and i.indschema=i.tabschema
                and i.uniquerule <> 'P' -- skip primary keys
        ORDER BY i.tabschema, i.tabname, i.indname
        --fetch first 1000 rows only
)
, CTEIncludeColumns as (
        select
        substr(xmlserialize(xmlagg(xmltext(concat(', ', IC.colname)) order by IC.colseq) as varchar(32000)), length(', ')+1) as csv_columns
        ,C.tabschema
        ,C.tabname
        ,IC.indname
        from syscat.columns as C
        inner join syscat.indexcoluse as IC on C.tabschema=IC.indschema and C.colname=IC.colname
        inner join CTEListNonPrimaryIndexes on C.tabschema=CTEListNonPrimaryIndexes.tabschema and C.tabname=CTEListNonPrimaryIndexes.tabname
                and IC.indname=CTEListNonPrimaryIndexes.indname --and IC.colorder='I'
        where IC.colorder='I'
        group by C.tabschema, C.tabname, IC.indname
)
select CTEListNonPrimaryIndexes.tabschema as "{SCHEMA}"
, CTEListNonPrimaryIndexes.tabname as "{TABLE_NAME}"
, CTEListNonPrimaryIndexes.indname as "{INDEX_NAME}"
, varchar_format(I.LASTUSED, 'MM/DD/YYYY') as "{LAST_USED}"
, (select substr(xmlserialize(xmlagg(xmltext(concat(', ', IC.colname||' '
        ||case when IC.colorder='A' then 'asc' else 'desc' end
        )) order by IC.colseq) as varchar(32000)), length(', ')+1)
        from syscat.columns as C
        inner join syscat.indexcoluse as IC on C.tabschema=IC.indschema and C.colname=IC.colname
        where C.tabschema=CTEListNonPrimaryIndexes.tabschema and C.tabname=CTEListNonPrimaryIndexes.tabname and IC.indname=I.indname
        and IC.colorder in ('A', 'D') -- ascending or descending, exclude include columns. include columns are listed separately.
        ) as "{INDEX_COLUMNS_CSV}"
, case when CTEIncludeColumns.csv_columns is not null then 'include ('||CTEIncludeColumns.csv_columns||')' end as "{INCLUDE_COLUMNS_CSV}"
from CTEListNonPrimaryIndexes
inner join syscat.indexes as I on CTEListNonPrimaryIndexes.tabschema=I.tabschema and CTEListNonPrimaryIndexes.tabname=I.tabname and CTEListNonPrimaryIndexes.tabschema=I.indschema
        and CTEListNonPrimaryIndexes.indname=I.indname
left outer join CTEIncludeColumns on CTEListNonPrimaryIndexes.tabschema=CTEIncludeColumns.tabschema
        and CTEListNonPrimaryIndexes.tabname=CTEIncludeColumns.tabname
        and CTEListNonPrimaryIndexes.indname=CTEIncludeColumns.indname
--where I.LASTUSED >= '2014-08-04' -- retrieve all indexes used after 8/4/2014
--where I.LASTUSED >= current_timestamp - 30 days -- retrieve all indexes used in the last 30 days
order by CTEListNonPrimaryIndexes.tabschema, CTEListNonPrimaryIndexes.tabname, CTEListNonPrimaryIndexes.indname
;

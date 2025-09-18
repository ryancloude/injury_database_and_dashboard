{% macro apply_transactions_overrides(schema='silver', table='transactions') %}
  {% set fqtn = '"' ~ schema | replace('"','""') ~ '"."' ~ table | replace('"','""') ~ '"' %}

    {% set sql %}
    begin;

    -- Updated José Fernández transaction to reflect his passing
    update {{ fqtn }}
    set typecode = 'DEC',
        typedesc = 'Deceased'
    where trans_id = 245302;

    -- Delete incorrect Will Banfield IL placement (ALT site, not MLB)
    delete from {{ fqtn }}
    where trans_id = 446539;

    -- Add activation for Tyler Soderstrom (IL 2024-07-11 to 2024-09-13)
    insert into {{ fqtn }} 
        (trans_id, "date", effectivedate, resolutiondate, typecode, typedesc, description, person_id, toteam_id, fromteam_id)
    values
        (999999, '2024-09-14', '2024-09-14', '2024-09-14', 'SC', 'Status Change',
         'Oakland Athletics activated 1B Tyler Soderstrom from 10-day injured list', 691016, 133, null)
    on conflict (trans_id) do update
        set "date"         = excluded."date",
            effectivedate = excluded.effectivedate,
            resolutiondate = excluded.resolutiondate,
            typecode       = excluded.typecode,
            typedesc       = excluded.typedesc,
            description    = excluded.description,
            person_id      = excluded.person_id,
            toteam_id      = excluded.toteam_id,
            fromteam_id    = excluded.fromteam_id;

    -- Fix bad effective_date
    update {{ fqtn }}
    set effectivedate = '2025-02-20'
    where trans_id = 816212;

    -- Add activation for Jacob Wilson (IL 2024-07-20 to 2024-08-26)
    insert into {{ fqtn }} 
        (trans_id, "date", effectivedate, resolutiondate, typecode, typedesc, description, person_id, toteam_id, fromteam_id)
    values
        (999998, '2024-08-27', '2024-08-27', '2024-08-27', 'SC', 'Status Change',
         'Oakland Athletics activated SS Jacob Wilson from 10-day injured list.', 805779, 133, null)
    on conflict (trans_id) do update
        set "date"         = excluded."date",
            effectivedate = excluded.effectivedate,
            resolutiondate = excluded.resolutiondate,
            typecode       = excluded.typecode,
            typedesc       = excluded.typedesc,
            description    = excluded.description,
            person_id      = excluded.person_id,
            toteam_id      = excluded.toteam_id,
            fromteam_id    = excluded.fromteam_id;

    -- Chris Archer: correct IL effective_date; remove now-invalid activation
    update {{ fqtn }}
    set effectivedate = '2018-06-05'
    where trans_id = 358140;

    delete from {{ fqtn }}
    where trans_id = 367424;

    -- Hisashi Iwakuma: correct effective_date
    update {{ fqtn }}
    set effectivedate = '2015-04-21'
    where trans_id = 223188;


    --Fixing Austim Adams repeated IL placements and Activations in 2021-2022
    delete from {{ fqtn }}
    where trans_id = 476708;

    delete from {{ fqtn }}
    where trans_id = 477163;

    delete from {{ fqtn }}
    where trans_id = 614721;

    delete from {{ fqtn }}
    where trans_id = 614722;

    update {{ fqtn }}
    set description = 'San Diego Padres placed RHP Austin Adams on the 60-day injured list.'
    where trans_id = 617778;


    --Corrected Pablo Sandoval 2016 IL placement effective date
    update {{ fqtn }}
    set effectivedate = '2016-04-11'
    where trans_id = 262085;

    commit;
    {% endset %}

    {% do run_query(sql) %}
{% endmacro %}
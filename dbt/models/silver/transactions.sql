{{ config(
    materialized='incremental',
    schema='silver',
    alias='transactions',
    incremental_strategy='merge',
    unique_key=['trans_id'],
    post_hook=[
      "
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint
          WHERE conname = 'transactions_pk_key'
            AND conrelid = '{{ this }}'::regclass
        ) THEN
          EXECUTE 'ALTER TABLE {{ this }} ADD CONSTRAINT transactions_pk_key
                   PRIMARY KEY (trans_id)';
        END IF;
      END $$;
      ",
      "{{ apply_transactions_overrides(schema=this.schema, table=this.identifier) }}"
    ]
) }}


with src as (
  -- Reuse staging instead of re-writing transforms
  select * from {{ ref('stg_transactions') }}
)

select *
from src

{% if is_incremental() %}
  -- Sliding window to pick up late corrections; change 14 -> 7/21/etc. as you like
  where date >= current_date - interval '7 day'
{% endif %}
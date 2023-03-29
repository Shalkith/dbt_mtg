{% snapshot mtg_oracle__history %}
{{
    config(
      target_database='mtg',
      target_schema='mtg',
      unique_key="id",
      strategy='check',
      check_cols='all',

      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('mtg', 'mtg_oracle') }}

{% endsnapshot %}

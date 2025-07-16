{% raw %}{{config(
        materialized="incremental",
        unique_key="_id",
        tags=["nightly", "adserver", "backfill_discouraged"],
        post_hook=["{{ generate_manifest() }}"],
    )
}}{% endraw %}


{{ columns }}
{{ input_sql }}


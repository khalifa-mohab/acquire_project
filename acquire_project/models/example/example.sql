SELECT * FROM {{ source('prism_acquire', 'sessions') }}

{{ config(materialized='table') }}
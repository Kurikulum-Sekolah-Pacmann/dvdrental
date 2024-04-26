select * 
from {{ source("dwh", "country") }}
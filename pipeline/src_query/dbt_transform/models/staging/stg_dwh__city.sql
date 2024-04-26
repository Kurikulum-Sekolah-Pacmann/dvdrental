select * 
from {{ source("dwh", "city") }}
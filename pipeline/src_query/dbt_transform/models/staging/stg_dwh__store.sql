select * 
from {{ source("dwh", "store") }}
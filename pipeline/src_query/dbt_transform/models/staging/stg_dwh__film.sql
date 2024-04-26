select * 
from {{ source("dwh", "film") }}
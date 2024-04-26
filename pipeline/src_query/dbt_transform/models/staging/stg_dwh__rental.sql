select * 
from {{ source("dwh", "rental") }}
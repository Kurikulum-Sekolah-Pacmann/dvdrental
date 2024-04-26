select * 
from {{ source("dwh", "staff") }}
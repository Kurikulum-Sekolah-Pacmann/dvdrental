select * 
from {{ source("dwh", "customer") }}
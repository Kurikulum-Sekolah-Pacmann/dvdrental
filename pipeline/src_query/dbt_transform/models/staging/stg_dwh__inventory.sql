select * 
from {{ source("dwh", "inventory") }}
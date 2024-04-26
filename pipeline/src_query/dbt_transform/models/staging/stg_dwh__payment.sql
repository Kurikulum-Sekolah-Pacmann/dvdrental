select * 
from {{ source("dwh", "payment") }}
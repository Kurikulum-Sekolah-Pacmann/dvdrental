select * 
from {{ source("dwh", "film_category") }}
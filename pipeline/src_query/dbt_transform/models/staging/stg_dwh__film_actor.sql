select * 
from {{ source("dwh", "film_actor") }}
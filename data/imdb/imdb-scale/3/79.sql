SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.id=mk.movie_id AND ci.person_id>1323513 AND ci.role_id=2 AND mk.keyword_id=53929;
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2003 AND mi.info_type_id=4 AND mk.keyword_id=1417;
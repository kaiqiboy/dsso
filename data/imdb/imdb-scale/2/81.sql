SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7;
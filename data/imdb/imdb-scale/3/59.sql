SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci,movie_info mi WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<2000 AND mc.company_id<1451 AND mc.company_type_id>1 AND ci.person_id<2800476;
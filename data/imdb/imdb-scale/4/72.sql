SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mc.company_id<145 AND mc.company_type_id<2 AND ci.person_id>1174178 AND mi_idx.info_type_id<101;
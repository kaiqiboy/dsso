SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<4 AND ci.person_id<1554285 AND ci.role_id<3;
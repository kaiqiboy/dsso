SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.id=mk.movie_id AND ci.person_id<1015782 AND ci.role_id<2 AND mi.info_type_id<16;
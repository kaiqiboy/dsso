SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci,movie_info mi,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi_idx.info_type_id>100;